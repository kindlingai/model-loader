package catalog

import "testing"

// The deploy trees' catalogs parse, and give the release's width files,
// and only those, small segments.
func TestDeployCatalogsParse(t *testing.T) {
	for _, p := range []string{"../../deploy/compose/catalog.yaml", "../../deploy/k8s/examples/gr8/catalog.yaml"} {
		c, err := Load(p)
		if err != nil {
			t.Fatal(p, err)
		}
		m := c.Models[0]
		if m.SegmentSizeFor("experts/exlr8-layer-003.k4.safetensors") != 65536 || m.SegmentSizeFor("experts/exlr8-layer-003.index.safetensors") != 0 {
			t.Fatalf("%s: segment sizes %+v", p, m.SegmentSizes)
		}
	}
}
