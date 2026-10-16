package dryrun

import (
	"fmt"
	"strings"
)

const diffContext = 3

type diffToken struct {
	kind byte   // ' ' common, '-' removed, '+' added
	old  int    // 1-based old line number, 0 when absent
	new  int    // 1-based new line number, 0 when absent
	line string
}

// unifiedDiff produces a deterministic minimal unified diff (three lines of
// context) between old and new content. It is intentionally dependency-free
// and aimed at the small text payloads (certificates, kubeconfigs, passwd
// files) handled during bootstrap reconciliation.
func unifiedDiff(name string, old, new []byte) string {
	tokens := diffTokens(splitLines(string(old)), splitLines(string(new)))
	if len(tokens) == 0 {
		return ""
	}
	hunks := diffHunks(tokens)
	if len(hunks) == 0 {
		return ""
	}

	var b strings.Builder
	fmt.Fprintf(&b, "--- %s (on disk)\n", name)
	fmt.Fprintf(&b, "+++ %s (proposed)\n", name)
	for _, h := range hunks {
		fmt.Fprintf(&b, "@@ -%s +%s @@\n", formatHunkRange(h.oldStart, h.oldCount), formatHunkRange(h.newStart, h.newCount))
		for _, t := range h.tokens {
			fmt.Fprintf(&b, "%c%s\n", t.kind, t.line)
		}
	}
	return b.String()
}

type diffHunk struct {
	tokens                                  []diffToken
	oldStart, oldCount, newStart, newCount int
}

// diffHunks groups change tokens with up to diffContext common lines on each
// side, merging adjacent groups whose context overlaps, and computes the
// line ranges of both sides (insertion/deletion-only sides use the "N,0" form).
func diffHunks(tokens []diffToken) []diffHunk {
	changeIdx := []int{}
	for i, t := range tokens {
		if t.kind != ' ' {
			changeIdx = append(changeIdx, i)
		}
	}
	if len(changeIdx) == 0 {
		return nil
	}

	groups := [][2]int{}
	head, tail := changeIdx[0]-diffContext, changeIdx[0]+diffContext
	for _, idx := range changeIdx[1:] {
		l, r := idx-diffContext, idx+diffContext
		if l <= tail+1 {
			if r > tail {
				tail = r
			}
			continue
		}
		groups = append(groups, [2]int{head, tail})
		head, tail = l, r
	}
	groups = append(groups, [2]int{head, tail})

	hunks := make([]diffHunk, 0, len(groups))
	for _, g := range groups {
		l, r := g[0], g[1]
		if l < 0 {
			l = 0
		}
		if r >= len(tokens) {
			r = len(tokens) - 1
		}
		ht := tokens[l : r+1]
		h := diffHunk{tokens: ht}

		// Line count before the hunk gives the insertion point when one side
		// contributes no lines.
		for _, t := range tokens[:l] {
			if t.kind != '+' {
				h.oldStart++
			}
			if t.kind != '-' {
				h.newStart++
			}
		}
		for _, t := range ht {
			switch t.kind {
			case ' ':
				if h.oldCount == 0 {
					h.oldStart = t.old
				}
				if h.newCount == 0 {
					h.newStart = t.new
				}
				h.oldCount++
				h.newCount++
			case '-':
				if h.oldCount == 0 {
					h.oldStart = t.old
				}
				h.oldCount++
			case '+':
				if h.newCount == 0 {
					h.newStart = t.new
				}
				h.newCount++
			}
		}
		hunks = append(hunks, h)
	}
	return hunks
}

// formatHunkRange renders one side of a unified diff hunk range.
func formatHunkRange(start, count int) string {
	switch {
	case count == 0:
		return fmt.Sprintf("%d,0", start)
	case count == 1:
		return fmt.Sprintf("%d", start)
	default:
		return fmt.Sprintf("%d,%d", start, count)
	}
}

// diffTokens computes the forward edit script via an LCS dynamic program.
func diffTokens(oldLines, newLines []string) []diffToken {
	n, m := len(oldLines), len(newLines)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			if oldLines[i-1] == newLines[j-1] {
				lcs[i][j] = lcs[i-1][j-1] + 1
			} else if lcs[i-1][j] >= lcs[i][j-1] {
				lcs[i][j] = lcs[i-1][j]
			} else {
				lcs[i][j] = lcs[i][j-1]
			}
		}
	}

	tokens := []diffToken{}
	i, j := n, m
	for i > 0 || j > 0 {
		switch {
		case i > 0 && j > 0 && oldLines[i-1] == newLines[j-1]:
			tokens = append(tokens, diffToken{kind: ' ', old: i, new: j, line: oldLines[i-1]})
			i--
			j--
		case j > 0 && (i == 0 || lcs[i][j-1] >= lcs[i-1][j]):
			tokens = append(tokens, diffToken{kind: '+', old: 0, new: j, line: newLines[j-1]})
			j--
		default:
			tokens = append(tokens, diffToken{kind: '-', old: i, new: 0, line: oldLines[i-1]})
			i--
		}
	}
	reverseTokens(tokens)
	return tokens
}

// splitLines splits on '\n' without appending a synthetic trailing line and
// strips a single trailing '\r' so CRLF inputs stay stable.
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\r")
	}
	// strings.Split appends a final empty element for a trailing newline,
	// which is not a real line.
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func reverseTokens(t []diffToken) {
	for i, j := 0, len(t)-1; i < j; i, j = i+1, j-1 {
		t[i], t[j] = t[j], t[i]
	}
}
