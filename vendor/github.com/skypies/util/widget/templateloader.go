package widget

import(
	"html/template"
	"io/ioutil"
	"os"
	"strings"
)

// ParseRecursive walks the directory structure, loading all the files
// it finds. Will panic on failure. Follows symlinks.
func ParseRecursive(t *template.Template, dir string) *template.Template {
	dirStack := []string{dir}
	fileStack := []string{}

	for {
		if len(dirStack) == 0 { break }

		dir := dirStack[0]
		dirStack = dirStack[1:]

		contents,err := ioutil.ReadDir(dir)
		if err != nil { panic(err) }
		
		for _,f := range contents {
			if strings.HasPrefix(f.Name(), ".") {
				continue
			}
			if f.IsDir() || (f.Mode() & os.ModeSymlink != 0) {
				dirStack = append(dirStack, dir+"/"+f.Name())
			} else {
				fileStack = append(fileStack, dir+"/"+f.Name())
			}
		}
	}

	return template.Must(t.ParseFiles(fileStack...))
}
