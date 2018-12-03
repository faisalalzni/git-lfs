package gitattr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcessLinesWithMacros(t *testing.T) {
	lines, _, err := ParseLines(strings.NewReader(strings.Join([]string{
		"[attr]lfs filter=lfs diff=lfs merge=lfs -text",
		"*.dat lfs",
		"*.txt text"}, "\n")))

	assert.Len(t, lines, 3)
	assert.NoError(t, err)

	mp := NewMacroProcessor()
	lines = mp.ProcessLines(lines, true)

	assert.Len(t, lines, 2)

	assert.Equal(t, lines[0].Macro, "")
	assert.Equal(t, lines[0].Pattern.String(), "*.dat")
	assert.Len(t, lines[0].Attrs, 5)
	assert.Equal(t, lines[0].Attrs[0], &Attr{K: "filter", V: "lfs"})
	assert.Equal(t, lines[0].Attrs[1], &Attr{K: "diff", V: "lfs"})
	assert.Equal(t, lines[0].Attrs[2], &Attr{K: "merge", V: "lfs"})
	assert.Equal(t, lines[0].Attrs[3], &Attr{K: "text", V: "false"})
	assert.Equal(t, lines[0].Attrs[4], &Attr{K: "lfs", V: "true"})

	assert.Equal(t, lines[1].Macro, "")
	assert.Equal(t, lines[1].Pattern.String(), "*.txt")
	assert.Len(t, lines[1].Attrs, 1)
	assert.Equal(t, lines[1].Attrs[0], &Attr{K: "text", V: "true"})
}

func TestProcessLinesWithMacrosDisabled(t *testing.T) {
	lines, _, err := ParseLines(strings.NewReader(strings.Join([]string{
		"[attr]lfs filter=lfs diff=lfs merge=lfs -text",
		"*.dat lfs",
		"*.txt text"}, "\n")))

	assert.Len(t, lines, 3)
	assert.NoError(t, err)

	mp := NewMacroProcessor()
	lines = mp.ProcessLines(lines, false)

	assert.Len(t, lines, 2)

	assert.Equal(t, lines[0].Macro, "")
	assert.Equal(t, lines[0].Pattern.String(), "*.dat")
	assert.Len(t, lines[0].Attrs, 1)
	assert.Equal(t, lines[0].Attrs[0], &Attr{K: "lfs", V: "true"})

	assert.Equal(t, lines[1].Macro, "")
	assert.Equal(t, lines[1].Pattern.String(), "*.txt")
	assert.Len(t, lines[1].Attrs, 1)
	assert.Equal(t, lines[1].Attrs[0], &Attr{K: "text", V: "true"})
}

func TestProcessLinesWithBinaryMacros(t *testing.T) {
	lines, _, err := ParseLines(strings.NewReader(strings.Join([]string{
		"*.dat binary",
		"*.txt text"}, "\n")))

	assert.Len(t, lines, 2)
	assert.NoError(t, err)

	mp := NewMacroProcessor()
	lines = mp.ProcessLines(lines, true)

	assert.Len(t, lines, 2)

	assert.Equal(t, lines[0].Macro, "")
	assert.Equal(t, lines[0].Pattern.String(), "*.dat")
	assert.Len(t, lines[0].Attrs, 4)
	assert.Equal(t, lines[0].Attrs[0], &Attr{K: "diff", V: "false"})
	assert.Equal(t, lines[0].Attrs[1], &Attr{K: "merge", V: "false"})
	assert.Equal(t, lines[0].Attrs[2], &Attr{K: "text", V: "false"})
	assert.Equal(t, lines[0].Attrs[3], &Attr{K: "binary", V: "true"})

	assert.Equal(t, lines[1].Macro, "")
	assert.Equal(t, lines[1].Pattern.String(), "*.txt")
	assert.Len(t, lines[1].Attrs, 1)
	assert.Equal(t, lines[1].Attrs[0], &Attr{K: "text", V: "true"})
}

func TestProcessLinesIsStateful(t *testing.T) {
	lines, _, err := ParseLines(strings.NewReader(strings.Join([]string{
		"[attr]lfs filter=lfs diff=lfs merge=lfs -text",
		"*.txt text"}, "\n")))

	assert.Len(t, lines, 2)
	assert.NoError(t, err)

	mp := NewMacroProcessor()
	lines = mp.ProcessLines(lines, true)

	assert.Len(t, lines, 1)

	assert.Equal(t, lines[0].Macro, "")
	assert.Equal(t, lines[0].Pattern.String(), "*.txt")
	assert.Len(t, lines[0].Attrs, 1)
	assert.Equal(t, lines[0].Attrs[0], &Attr{K: "text", V: "true"})

	lines2, _, err := ParseLines(strings.NewReader("*.dat lfs\n"))

	assert.Len(t, lines2, 1)
	assert.NoError(t, err)

	lines2 = mp.ProcessLines(lines2, false)

	assert.Len(t, lines2, 1)

	assert.Equal(t, lines2[0].Macro, "")
	assert.Equal(t, lines2[0].Pattern.String(), "*.dat")
	assert.Len(t, lines2[0].Attrs, 5)
	assert.Equal(t, lines2[0].Attrs[0], &Attr{K: "filter", V: "lfs"})
	assert.Equal(t, lines2[0].Attrs[1], &Attr{K: "diff", V: "lfs"})
	assert.Equal(t, lines2[0].Attrs[2], &Attr{K: "merge", V: "lfs"})
	assert.Equal(t, lines2[0].Attrs[3], &Attr{K: "text", V: "false"})
	assert.Equal(t, lines2[0].Attrs[4], &Attr{K: "lfs", V: "true"})
}
