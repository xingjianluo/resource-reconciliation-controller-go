package reconciler

import (
	"os"
	"strings"
)

func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func matchTmp(name string) bool {
	return strings.HasPrefix(name, ".snap-") && strings.HasSuffix(name, ".tmp")
}
