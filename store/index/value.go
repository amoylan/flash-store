package index

type IndexValueSet map[string]struct{}

func (o IndexValueSet) Intersect(b IndexValueSet) IndexValueSet {
	if len(o) == 0 || len(b) == 0 {
		return make(IndexValueSet)
	}
	var small, large = b, o
	if len(o) < len(b) {
		small = o
		large = b
	}
	result := make(IndexValueSet)
	for k := range small {
		if _, exists := large[k]; exists {
			result[k] = struct{}{}
		}
	}
	return result
}

func (o IndexValueSet) Merge(b IndexValueSet) IndexValueSet {
	if len(o) == 0 {
		return b
	}
	for k, v := range b {
		o[k] = v
	}
	return o
}
