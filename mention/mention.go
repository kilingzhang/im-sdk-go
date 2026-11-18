// Package mention locates and renders @name mentions in outbound text while
// ignoring code blocks, inline code spans, email addresses, and ambiguous names.
package mention

import (
	"cmp"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

// Spot marks the byte range [Start, End) of one matched @name in text.
type Spot struct {
	Start   int
	End     int
	Mention imsdk.Mention
}

// Candidate represents one contact in a group that @name resolution considers.
type Candidate struct {
	PlatformID string
	ContactID  string
	PersonName string
	Alias      string
	Name       string
	Spoke      bool
}

// MentionedContact records a resolved contact mention for reporting back to the caller.
type MentionedContact struct {
	Contact string
	Name    string
}

// Resolved holds the resolved platform mentions, contact summaries, and unmatched @words.
type Resolved struct {
	Mentions  []imsdk.Mention
	Mentioned []MentionedContact
	Unmatched []string
}

var (
	codePattern      = regexp.MustCompile("(?s)```.*?```|```.*$|`[^`\\n]*`")
	unmatchedPattern = regexp.MustCompile(`(?:^|[^A-Za-z0-9._%+-])@([^\s@,，。、!！?？:：;；()（）\[\]{}<>"'` + "`" + `]{1,32})`)
	everyoneWords    = map[string]struct{}{
		"all":      {},
		"everyone": {},
		"here":     {},
		"channel":  {},
		"所有人":      {},
		"全体成员":     {},
	}
)

func isWordByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

func isAddressByte(b byte) bool {
	return isWordByte(b) || b == '.' || b == '%' || b == '+' || b == '-'
}

// CodeRanges returns byte intervals [from, to) of fenced code blocks and inline code spans.
func CodeRanges(text string) [][2]int {
	locs := codePattern.FindAllStringIndex(text, -1)
	if len(locs) == 0 {
		return nil
	}
	out := make([][2]int, len(locs))
	for i, loc := range locs {
		out[i] = [2]int{loc[0], loc[1]}
	}
	return out
}

func inRanges(pos int, ranges [][2]int) bool {
	for _, r := range ranges {
		if pos >= r[0] && pos < r[1] {
			return true
		}
	}
	return false
}

// Spots finds every @name in text matching one of mentions.
// Longest name wins; case-insensitive; ASCII word endings require a word boundary;
// CJK names may be followed immediately by text; code blocks and email addresses are skipped.
func Spots(text string, mentions []imsdk.Mention) []Spot {
	if len(mentions) == 0 || !strings.Contains(text, "@") {
		return nil
	}
	sorted := make([]imsdk.Mention, 0, len(mentions))
	for _, m := range mentions {
		if m.Name != "" {
			sorted = append(sorted, m)
		}
	}
	slices.SortStableFunc(sorted, func(a, b imsdk.Mention) int {
		return cmp.Compare(utf8.RuneCountInString(b.Name), utf8.RuneCountInString(a.Name))
	})
	if len(sorted) == 0 {
		return nil
	}

	code := CodeRanges(text)
	var spots []Spot
	for i := strings.IndexByte(text, '@'); i != -1; {
		next := i + 1
		prevAddr := i > 0 && isAddressByte(text[i-1])
		if !inRanges(i, code) && !prevAddr {
			for _, m := range sorted {
				end := i + 1 + len(m.Name)
				if end > len(text) {
					continue
				}
				if !strings.EqualFold(text[i+1:end], m.Name) {
					continue
				}
				lastByte := m.Name[len(m.Name)-1]
				if lastByte < utf8.RuneSelf && isWordByte(lastByte) && end < len(text) && text[end] < utf8.RuneSelf && isWordByte(text[end]) {
					continue
				}
				spots = append(spots, Spot{Start: i, End: end, Mention: m})
				next = end
				break
			}
		}
		rel := strings.IndexByte(text[next:], '@')
		if rel < 0 {
			break
		}
		i = next + rel
	}
	return spots
}

// Render rewrites every matched @name spot in text with render(mention), leaving everything else untouched.
func Render(text string, mentions []imsdk.Mention, render func(imsdk.Mention) string) string {
	spots := Spots(text, mentions)
	if len(spots) == 0 {
		return text
	}
	var b strings.Builder
	at := 0
	for _, spot := range spots {
		b.WriteString(text[at:spot.Start])
		b.WriteString(render(spot.Mention))
		at = spot.End
	}
	b.WriteString(text[at:])
	return b.String()
}

// Resolve matches @names in text against group candidates, disambiguating duplicate names
// by who has spoken in the chat and reporting up to 5 unmatched @words.
func Resolve(text string, candidates []Candidate) Resolved {
	if !strings.Contains(text, "@") {
		return Resolved{}
	}
	type holderMap struct {
		order []string
		byID  map[string]Candidate
	}
	byName := make(map[string]*holderMap)
	var nameKeys []string

	for _, c := range candidates {
		for _, raw := range []string{c.PersonName, c.Alias, c.Name} {
			name := strings.TrimSpace(raw)
			if name == "" {
				continue
			}
			key := strings.ToLower(name)
			hm := byName[key]
			if hm == nil {
				hm = &holderMap{byID: make(map[string]Candidate)}
				byName[key] = hm
				nameKeys = append(nameKeys, key)
			}
			if _, exists := hm.byID[c.PlatformID]; !exists {
				hm.order = append(hm.order, c.PlatformID)
			}
			hm.byID[c.PlatformID] = c
		}
	}

	type option struct {
		imsdk.Mention
		contact string
	}
	var options []option
	var mentionList []imsdk.Mention
	contactByKey := make(map[string]string)

	for _, key := range nameKeys {
		hm := byName[key]
		who := make([]Candidate, 0, len(hm.order))
		for _, pid := range hm.order {
			who = append(who, hm.byID[pid])
		}
		if len(who) > 1 {
			var spoke []Candidate
			for _, c := range who {
				if c.Spoke {
					spoke = append(spoke, c)
				}
			}
			who = spoke
		}
		if len(who) != 1 {
			continue
		}
		one := who[0]
		var matchedName string
		for _, raw := range []string{one.PersonName, one.Alias, one.Name} {
			n := strings.TrimSpace(raw)
			if strings.ToLower(n) == key {
				matchedName = n
				break
			}
		}
		if matchedName != "" {
			m := imsdk.Mention{ID: one.PlatformID, Name: matchedName}
			options = append(options, option{Mention: m, contact: one.ContactID})
			mentionList = append(mentionList, m)
			contactByKey[one.PlatformID+"\n"+matchedName] = one.ContactID
		}
	}

	spots := Spots(text, mentionList)
	seenMention := make(map[string]struct{})
	seenContact := make(map[string]struct{})
	var outMentions []imsdk.Mention
	var outMentioned []MentionedContact

	for _, s := range spots {
		mk := s.Mention.ID + "\n" + s.Mention.Name
		if _, ok := seenMention[mk]; !ok {
			seenMention[mk] = struct{}{}
			outMentions = append(outMentions, s.Mention)
		}
		cid := contactByKey[mk]
		if cid != "" {
			if _, ok := seenContact[cid]; !ok {
				seenContact[cid] = struct{}{}
				outMentioned = append(outMentioned, MentionedContact{Contact: cid, Name: s.Mention.Name})
			}
		}
	}

	taken := func(pos int) bool {
		for _, s := range spots {
			if pos >= s.Start && pos < s.End {
				return true
			}
		}
		return false
	}
	code := CodeRanges(text)
	seenUnmatched := make(map[string]struct{})
	var unmatched []string
	for _, idx := range unmatchedPattern.FindAllStringSubmatchIndex(text, -1) {
		wordStart, wordEnd := idx[2], idx[3]
		atPos := wordStart - 1
		word := text[wordStart:wordEnd]
		if taken(atPos) || inRanges(atPos, code) {
			continue
		}
		if _, skip := everyoneWords[strings.ToLower(word)]; skip {
			continue
		}
		tag := "@" + word
		if _, dup := seenUnmatched[tag]; !dup {
			seenUnmatched[tag] = struct{}{}
			unmatched = append(unmatched, tag)
			if len(unmatched) >= 5 {
				break
			}
		}
	}

	return Resolved{
		Mentions:  outMentions,
		Mentioned: outMentioned,
		Unmatched: unmatched,
	}
}
