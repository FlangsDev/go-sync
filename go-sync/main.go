package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	neo4jURI := flag.String("neo4j-uri", "neo4j://localhost:7687", "URI database Neo4j")
	neo4jUser := flag.String("neo4j-user", "neo4j", "Username Neo4j")
	neo4jPass := flag.String("neo4j-password", "", "Password Neo4j (kosong = pakai env NEO4J_PASSWORD)")
	timeout := flag.Duration("timeout", 30*time.Second, "batas waktu tiap operasi sync")
	debounce := flag.Duration("debounce", 2*time.Second, "jeda sebelum resync setelah event terakhir masuk")
	fallbackPoll := flag.Duration("fallback-poll", 5*time.Minute, "resync penuh berkala sebagai jaring pengaman (0 = mati)")
	flag.Parse()

	// Context utama, dibatalkan kalau user tekan Ctrl+C / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("gagal membuat Docker client: %w", err)
	}
	defer cli.Close()

	pingCtx, cancel := context.WithTimeout(ctx, *timeout)
	_, err = cli.Ping(pingCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("tidak bisa terhubung ke Docker daemon: %w", err)
	}

	// Password diutamakan dari flag; kalau kosong dibaca dari env NEO4J_PASSWORD
	// supaya secret tidak perlu ikut di argumen command (yang kelihatan di `ps`).
	password := *neo4jPass
	if password == "" {
		password = os.Getenv("NEO4J_PASSWORD")
	}
	if password == "" {
		password = "password"
	}

	auth := neo4j.BasicAuth(*neo4jUser, password, "")
	dbDriver, err := neo4j.NewDriverWithContext(*neo4jURI, auth)
	if err != nil {
		return fmt.Errorf("gagal koneksi ke Neo4j: %w", err)
	}
	defer dbDriver.Close(ctx)

	verifyCtx, cancel := context.WithTimeout(ctx, *timeout)
	err = dbDriver.VerifyConnectivity(verifyCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("gagal verifikasi koneksi Neo4j: %w", err)
	}

	// Pastikan constraint unik ada, biar MERGE cepat & konsisten
	if err := ensureConstraints(ctx, dbDriver, *timeout); err != nil {
		return fmt.Errorf("gagal membuat constraint: %w", err)
	}

	syncer := &Syncer{cli: cli, driver: dbDriver, timeout: *timeout}

	// 1. Full sync pertama kali saat program start
	fmt.Println("Melakukan full sync awal...")
	if err := syncer.FullSync(ctx); err != nil {
		return fmt.Errorf("gagal full sync awal: %w", err)
	}
	fmt.Println("Full sync awal selesai. Mendengarkan Docker events...")

	// 2. Subscribe ke Docker Events API (realtime, gratis, bawaan Docker)
	eventFilter := filters.NewArgs()
	for _, t := range []events.Type{
		events.ContainerEventType,
		events.NetworkEventType,
		events.ImageEventType,
	} {
		eventFilter.Add("type", string(t))
	}

	eventsCh, errCh := cli.Events(ctx, events.ListOptions{Filters: eventFilter})

	// Debounce channel: banyak event yang datang beruntun (misal `docker compose up`
	// bikin puluhan event dalam sedetik) di-gabung jadi satu trigger resync saja.
	trigger := make(chan struct{}, 1)
	var debounceTimer *time.Timer
	var mu sync.Mutex

	scheduleResync := func() {
		mu.Lock()
		defer mu.Unlock()
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
		debounceTimer = time.AfterFunc(*debounce, func() {
			select {
			case trigger <- struct{}{}:
			default:
			}
		})
	}

	// 3. Fallback polling sebagai jaring pengaman (opsional), jaga-jaga kalau
	// ada event yang terlewat/terputus koneksinya
	var fallbackTicker *time.Ticker
	var fallbackCh <-chan time.Time
	if *fallbackPoll > 0 {
		fallbackTicker = time.NewTicker(*fallbackPoll)
		defer fallbackTicker.Stop()
		fallbackCh = fallbackTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Menerima sinyal berhenti, keluar...")
			return nil

		case ev := <-eventsCh:
			handleEvent(ctx, syncer, ev, *timeout)
			scheduleResync()

		case err := <-errCh:
			if ctx.Err() != nil {
				continue // sedang shutdown, biarkan case ctx.Done() yang menutup
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "event stream terputus: %v, mencoba lagi dalam 5 detik...\n", err)
			} else {
				fmt.Fprintln(os.Stderr, "event stream tertutup, menyambung ulang dalam 5 detik...")
			}
			time.Sleep(5 * time.Second)
			eventsCh, errCh = cli.Events(ctx, events.ListOptions{Filters: eventFilter})
			// Sync ulang untuk mengejar event yang mungkin terlewat saat terputus
			scheduleResync()

		case <-trigger:
			fmt.Println("Perubahan terdeteksi, melakukan resync...")
			if err := syncer.FullSync(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "resync gagal: %v\n", err)
			} else {
				fmt.Println("Resync selesai.")
			}

		case <-fallbackCh:
			fmt.Println("Fallback poll: melakukan resync berkala...")
			if err := syncer.FullSync(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "fallback resync gagal: %v\n", err)
			}
		}
	}
}

// handleEvent menangani penghapusan langsung (destroy/remove) supaya node basi
// tidak nunggu resync penuh untuk hilang dari graph.
func handleEvent(ctx context.Context, s *Syncer, ev events.Message, timeout time.Duration) {
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	action := string(ev.Action)
	id := ev.Actor.ID

	switch ev.Type {
	case events.ContainerEventType:
		if action == "destroy" {
			_ = s.DeleteNode(opCtx, "Container", id)
		}
	case events.NetworkEventType:
		if action == "destroy" {
			_ = s.DeleteNode(opCtx, "Network", id)
		}
	case events.ImageEventType:
		if action == "delete" || action == "untag" {
			_ = s.DeleteNode(opCtx, "Image", id)
		}
	}
}

// ---------------------------------------------------------------------------
// Syncer: pembungkus koneksi Docker + Neo4j
// ---------------------------------------------------------------------------

type Syncer struct {
	cli     *client.Client
	driver  neo4j.DriverWithContext
	timeout time.Duration
}

func (s *Syncer) FullSync(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	containers, err := s.cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("gagal mengambil daftar container: %w", err)
	}
	images, err := s.cli.ImageList(ctx, image.ListOptions{All: true})
	if err != nil {
		return fmt.Errorf("gagal mengambil daftar image: %w", err)
	}
	networks, err := s.cli.NetworkList(ctx, network.ListOptions{})
	if err != nil {
		return fmt.Errorf("gagal mengambil daftar network: %w", err)
	}

	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// Token generasi unik per sync; setiap relationship yang masih valid akan
	// "distempel" dengan token ini, lalu relationship yang tidak ikut ter-stempel
	// (sudah tidak berlaku) akan di-prune di dalam ingestRelationships.
	gen := time.Now().UnixNano()

	if err := ingestNodes(ctx, session, containers, images, networks); err != nil {
		return fmt.Errorf("gagal ingest nodes: %w", err)
	}
	if err := ingestRelationships(ctx, session, containers, gen); err != nil {
		return fmt.Errorf("gagal ingest relationships: %w", err)
	}

	// Bersihkan node yang sudah tidak ada lagi di Docker tapi masih nyangkut di Neo4j
	if err := pruneStale(ctx, session, "Container", idSet(containers, func(c types.Container) string { return c.ID })); err != nil {
		return fmt.Errorf("gagal prune container basi: %w", err)
	}
	if err := pruneStale(ctx, session, "Image", idSet(images, func(i image.Summary) string { return i.ID })); err != nil {
		return fmt.Errorf("gagal prune image basi: %w", err)
	}
	if err := pruneStale(ctx, session, "Network", idSet(networks, func(n network.Summary) string { return n.ID })); err != nil {
		return fmt.Errorf("gagal prune network basi: %w", err)
	}

	return nil
}

func (s *Syncer) DeleteNode(ctx context.Context, label, id string) error {
	if id == "" {
		return nil
	}
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	cypher := fmt.Sprintf(`MATCH (n:%s {id: $id}) DETACH DELETE n`, label)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, cypher, map[string]any{"id": id})
	})
	return err
}

// pruneStale menghapus node berlabel `label` yang id-nya sudah tidak ada di `liveIDs`.
func pruneStale(ctx context.Context, session neo4j.SessionWithContext, label string, liveIDs []string) error {
	cypher := fmt.Sprintf(`
		MATCH (n:%s)
		WHERE NOT n.id IN $liveIDs
		DETACH DELETE n
	`, label)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, cypher, map[string]any{"liveIDs": liveIDs})
	})
	return err
}

func idSet[T any](items []T, getID func(T) string) []string {
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, getID(it))
	}
	return ids
}

func ensureConstraints(ctx context.Context, driver neo4j.DriverWithContext, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	constraints := []string{
		"CREATE CONSTRAINT container_id IF NOT EXISTS FOR (c:Container) REQUIRE c.id IS UNIQUE",
		"CREATE CONSTRAINT image_id IF NOT EXISTS FOR (i:Image) REQUIRE i.id IS UNIQUE",
		"CREATE CONSTRAINT network_id IF NOT EXISTS FOR (n:Network) REQUIRE n.id IS UNIQUE",
	}
	for _, c := range constraints {
		if _, err := session.Run(ctx, c, nil); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Node Ingestors (sama seperti versi sebelumnya, sudah dibuat nil-safe)
// ---------------------------------------------------------------------------

func ingestNodes(ctx context.Context, session neo4j.SessionWithContext, containers []types.Container, images []image.Summary, networks []network.Summary) error {
	containerData := make([]map[string]any, 0, len(containers))
	for _, c := range containers {
		containerData = append(containerData, map[string]any{
			"id":      c.ID,
			"name":    containerName(c),
			"image":   c.Image,
			"status":  c.Status,
			"created": unixToISO(c.Created),
			"labels":  formatLabels(c.Labels),
		})
	}

	imageData := make([]map[string]any, 0, len(images))
	for _, img := range images {
		repo, tag := repoAndTag(img.RepoTags)
		imageData = append(imageData, map[string]any{
			"id":         img.ID,
			"repository": repo,
			"tag":        tag,
			"size":       img.Size,
			"created":    unixToISO(img.Created),
		})
	}

	networkData := make([]map[string]any, 0, len(networks))
	for _, n := range networks {
		networkData = append(networkData, map[string]any{
			"id":      n.ID,
			"name":    n.Name,
			"driver":  n.Driver,
			"scope":   n.Scope,
			"created": n.Created.UTC().Format(time.RFC3339),
		})
	}

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		cypherContainer := `
			UNWIND $data AS p
			MERGE (c:Container {id: p.id})
			SET c.name = p.name, c.image = p.image, c.status = p.status, c.created = p.created, c.labels = p.labels
		`
		if _, err := tx.Run(ctx, cypherContainer, map[string]any{"data": containerData}); err != nil {
			return nil, err
		}

		cypherImage := `
			UNWIND $data AS p
			MERGE (i:Image {id: p.id})
			SET i.repository = p.repository, i.tag = p.tag, i.size = p.size, i.created = p.created
		`
		if _, err := tx.Run(ctx, cypherImage, map[string]any{"data": imageData}); err != nil {
			return nil, err
		}

		cypherNetwork := `
			UNWIND $data AS p
			MERGE (n:Network {id: p.id})
			SET n.name = p.name, n.driver = p.driver, n.scope = p.scope, n.created = p.created
		`
		if _, err := tx.Run(ctx, cypherNetwork, map[string]any{"data": networkData}); err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

// ---------------------------------------------------------------------------
// Relationship Ingestors
// ---------------------------------------------------------------------------

func ingestRelationships(ctx context.Context, session neo4j.SessionWithContext, containers []types.Container, gen int64) error {
	relContainerImage := make([]map[string]any, 0)
	relContainerNetwork := make([]map[string]any, 0)
	networkMembers := make(map[string][]string)

	for _, c := range containers {
		if c.ImageID != "" {
			relContainerImage = append(relContainerImage, map[string]any{
				"container_id": c.ID,
				"image_id":     c.ImageID,
			})
		}

		if c.NetworkSettings != nil {
			for _, endpoint := range c.NetworkSettings.Networks {
				if endpoint == nil || endpoint.NetworkID == "" {
					continue
				}
				relContainerNetwork = append(relContainerNetwork, map[string]any{
					"container_id": c.ID,
					"network_id":   endpoint.NetworkID,
				})
				networkMembers[endpoint.NetworkID] = append(networkMembers[endpoint.NetworkID], c.ID)
			}
		}
	}

	relContainerContainer := make([]map[string]any, 0)
	netIDs := make([]string, 0, len(networkMembers))
	for id := range networkMembers {
		netIDs = append(netIDs, id)
	}
	sort.Strings(netIDs)

	for _, netID := range netIDs {
		members := networkMembers[netID]
		sort.Strings(members)
		for i := 0; i < len(members); i++ {
			for j := i + 1; j < len(members); j++ {
				if members[i] == members[j] {
					continue
				}
				relContainerContainer = append(relContainerContainer, map[string]any{
					"c1_id":      members[i],
					"c2_id":      members[j],
					"network_id": netID,
				})
			}
		}
	}

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		cypherRelImg := `
			UNWIND $data AS r
			MATCH (c:Container {id: r.container_id})
			MATCH (i:Image {id: r.image_id})
			MERGE (c)-[rel:USES_IMAGE]->(i)
			SET rel.syncGen = $gen
		`
		if _, err := tx.Run(ctx, cypherRelImg, map[string]any{"data": relContainerImage, "gen": gen}); err != nil {
			return nil, err
		}

		cypherRelNet := `
			UNWIND $data AS r
			MATCH (c:Container {id: r.container_id})
			MATCH (n:Network {id: r.network_id})
			MERGE (c)-[rel:CONNECTED_TO]->(n)
			SET rel.syncGen = $gen
		`
		if _, err := tx.Run(ctx, cypherRelNet, map[string]any{"data": relContainerNetwork, "gen": gen}); err != nil {
			return nil, err
		}

		cypherRelCc := `
			UNWIND $data AS r
			MATCH (c1:Container {id: r.c1_id})
			MATCH (c2:Container {id: r.c2_id})
			MERGE (c1)-[rel:SHARES_NETWORK]->(c2)
			SET rel.network_id = r.network_id, rel.syncGen = $gen
		`
		if _, err := tx.Run(ctx, cypherRelCc, map[string]any{"data": relContainerContainer, "gen": gen}); err != nil {
			return nil, err
		}

		// Turunan: sebuah image dianggap "berjalan di" network kalau ada container
		// yang memakai image itu sekaligus terhubung ke network tsb. Nama relasinya
		// RUNS_ON (bukan CONNECTED_TO) supaya tidak bertabrakan dengan relasi
		// Container->Network.
		cypherRelImgNet := `
			MATCH (i:Image)<-[:USES_IMAGE]-(c:Container)-[:CONNECTED_TO]->(n:Network)
			MERGE (i)-[rel:RUNS_ON]->(n)
			SET rel.syncGen = $gen
		`
		if _, err := tx.Run(ctx, cypherRelImgNet, map[string]any{"gen": gen}); err != nil {
			return nil, err
		}

		// Prune relationship basi: relasi yang tidak ikut ter-stempel gen sync
		// saat ini berarti sudah tidak berlaku lagi (mis. container di-`network
		// disconnect` tapi container-nya masih hidup) -> hapus. Ini yang bikin
		// graph tetap akurat, bukan cuma bertambah.
		for _, relType := range []string{"USES_IMAGE", "CONNECTED_TO", "SHARES_NETWORK", "RUNS_ON"} {
			cypherPrune := fmt.Sprintf(`
				MATCH ()-[r:%s]->()
				WHERE r.syncGen IS NULL OR r.syncGen <> $gen
				DELETE r
			`, relType)
			if _, err := tx.Run(ctx, cypherPrune, map[string]any{"gen": gen}); err != nil {
				return nil, err
			}
		}

		return nil, nil
	})

	return err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func containerName(c types.Container) string {
	if len(c.Names) == 0 {
		return ""
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

func repoAndTag(repoTags []string) (string, string) {
	if len(repoTags) == 0 {
		return "<none>", "<none>"
	}
	full := repoTags[0]
	idx := strings.LastIndex(full, ":")
	if idx == -1 || idx < strings.LastIndex(full, "/") {
		return full, "<none>"
	}
	return full[:idx], full[idx+1:]
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ";")
}

func unixToISO(sec int64) string {
	if sec == 0 {
		return ""
	}
	return time.Unix(sec, 0).UTC().Format(time.RFC3339)
}