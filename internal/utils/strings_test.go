package utils

import "testing"

func TestStripSpacesAndReverse(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "ascii_no_spaces", in: "abc", want: "cba"},
		{name: "ascii_strip_inner_spaces", in: "a b c", want: "cba"},
		{name: "ascii_strip_outer_spaces", in: "  abc  ", want: "cba"},
		{name: "ascii_tab_and_newline", in: "a\tb\nc", want: "cba"},
		{name: "chinese_mixed_spaces", in: "  你好 世界  ", want: "界世好你"},
		{name: "emoji_multibyte", in: "🚀 A 🌍 B ", want: "B🌍A🚀"},
		{name: "only_spaces", in: "   \t\n   ", want: ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := StripSpacesAndReverse(c.in)
			if got != c.want {
				t.Fatalf("StripSpacesAndReverse(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}
