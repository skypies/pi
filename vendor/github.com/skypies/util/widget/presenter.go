package widget

import "net/url"

type Presenter interface {
	Values() url.Values
	FromFormValues(v url.Values, stem string) error
}

// ValuesIntoTemplateParams converts a set of url.Values into template parameters, suitable
//  for initializing HTML form elements
func ValuesIntoTemplateParams(stem string, values url.Values, params map[string]interface{}) {
	if stem != "" { stem = stem+"_" }

	for k,vslice := range values {
		if len(vslice) == 1 {
			params[stem+k] = vslice[0]
		} else {
			params[stem+k] = vslice
		}
	}
}

func AddValues(addedTo url.Values, toBeAdded url.Values) {
	for k,vals := range toBeAdded {
		for _,v := range vals {
			addedTo.Add(k,v)
		}
	}
}

func AddPrefixedValues(addedTo url.Values, toBeAdded url.Values, prefix string) {
	for k,vals := range toBeAdded {
		for _,v := range vals {
			addedTo.Add(prefix+"_"+k, v)
		}
	}
}
