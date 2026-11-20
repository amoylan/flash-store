package registry

import (
	"strings"
	"testing"
)

func TestParseStoreTag_Variants(t *testing.T) {
	cases := []struct {
		raw  string
		want storeTag
	}{
		{"", storeTag{Skip: true}},
		{"-", storeTag{Skip: true}},
		{"name,num=1", storeTag{Name: "name", Num: 1, HasNum: true}},
		{"name,num=2,index", storeTag{Name: "name", Num: 2, HasNum: true, Indexed: true}},
		{"n,num=3,custom", storeTag{Name: "n", Num: 3, HasNum: true, Custom: true}},
		{"price,num=4,index,prec=8", storeTag{Name: "price", Num: 4, HasNum: true, Indexed: true, DecPrec: int32Ptr(8)}},
		{"s,num=5,max=255", storeTag{Name: "s", Num: 5, HasNum: true, StrMax: intPtr(255)}},
		{"s,num=6,default=hello", storeTag{Name: "s", Num: 6, HasNum: true, HasDefault: true, Default: "hello"}},
		{"s,num=7,default=", storeTag{Name: "s", Num: 7, HasNum: true, HasDefault: true, Default: ""}},
		{"name,  num = 8 ,  index ,  prec = 8 ", storeTag{Name: "name", Num: 8, HasNum: true, Indexed: true, DecPrec: int32Ptr(8)}},
	}
	for _, c := range cases {
		got, err := parseStoreTag(c.raw)
		if err != nil {
			t.Fatalf("parseStoreTag(%q): %v", c.raw, err)
		}
		if got.Skip != c.want.Skip || got.Name != c.want.Name ||
			got.Num != c.want.Num || got.HasNum != c.want.HasNum ||
			got.Indexed != c.want.Indexed || got.Custom != c.want.Custom ||
			got.HasDefault != c.want.HasDefault || got.Default != c.want.Default ||
			!int32PtrEq(got.DecPrec, c.want.DecPrec) || !intPtrEq(got.StrMax, c.want.StrMax) {
			t.Errorf("parseStoreTag(%q) = %+v, want %+v", c.raw, got, c.want)
		}
	}
}

func TestParseStoreTag_Errors(t *testing.T) {
	bads := []struct {
		raw     string
		errPart string
	}{
		{",num=1,index", "empty name"},
		{"name,num=1,unknown", "unknown option"},
		{"name,num=1,prec", "prec requires"},
		{"name,num=1,prec=abc", "prec"},
		{"name,num=1,max", "max requires"},
		{"name,num=1,default", "default requires"},
		{"name", "missing required `num"},  // 缺 num=
		{"name,num=", "num requires"},      // num 没值
		{"name,num=abc", "num"},            // num 非整数
		{"name,num=0", "num must be > 0"},  // num=0 非法
		{"name,num=-3", "num must be > 0"}, // 负数非法
	}
	for _, c := range bads {
		_, err := parseStoreTag(c.raw)
		if err == nil {
			t.Errorf("parseStoreTag(%q): want error", c.raw)
			continue
		}
		if !strings.Contains(err.Error(), c.errPart) {
			t.Errorf("parseStoreTag(%q): error %q missing %q", c.raw, err, c.errPart)
		}
	}
}

func int32Ptr(n int32) *int32 { return &n }
func intPtr(n int) *int       { return &n }
func int32PtrEq(a, b *int32) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
func intPtrEq(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
