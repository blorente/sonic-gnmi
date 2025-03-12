package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"text/template"
)

var (
	//go:embed templates/*.tmpl
	ts     embed.FS
	inname = flag.String("in", "", "path to the input file")
	outdir = flag.String("out", ".", "the directory to write the output files into")
)

type ElementKey struct {
	Name string
	Val  string
}

type Element struct {
	Name     string
	Prefix   string
	Type     string
	Val      string
	Pattern  string
	Key      ElementKey
	Children []*Element
	Parent   *Element
	Skip     bool
	Result   int
}

type Val struct {
	Type    string      `json:type`
	Pattern string      `json:pattern`
	Val     interface{} `json:val`
}

type Key struct {
	Name string      `json:name`
	Val  interface{} `json:val`
}
type Elem struct {
	Prefix string `json:prefix`
	Elem   string `json:elem`
	Key    *Key   `json:key`
	Skip   bool   `json:skip`
	Result int    `json:result`
}

type Path struct {
	Desc     string `json:desc`
	Path     []Elem `json:path`
	Expected Val    `json:val`
	Config   bool   `json:config`
}

type Paths struct {
	Paths []Path `json:paths`
}

func processJSON(paths Paths) []*Element {
	ret := []*Element{}
	for _, path := range paths.Paths {
		var parent *Element
		var curr *Element
		for _, elem := range path.Path {
			curr = &Element{Name: elem.Elem, Prefix: elem.Prefix, Type: "object", Val: "", Skip: elem.Skip, Result: elem.Result}
			if elem.Key != nil {
				curr.Key = ElementKey{Name: elem.Key.Name, Val: fmt.Sprintf("%v", elem.Key.Val)}
				curr.Type = "array"
			}
			if parent == nil {
				ret = append(ret, curr)
				parent = curr
				continue
			}
			curr.Parent = parent
			parent.Children = append(parent.Children, curr)
			parent = curr
		}
		curr.Type = path.Expected.Type
		curr.Val = fmt.Sprintf("%v", path.Expected.Val)
		curr.Pattern = fmt.Sprintf("%v", path.Expected.Pattern)
	}
	return ret
}

func main() {
	flag.Parse()

	// Open input stream.
	in := os.Stdin
	var err error
	if *inname != "" {
		if in, err = os.Open(*inname); err != nil {
			log.Fatalln(err)
		}
	}
	defer in.Close()

	// Read the list of paths to be tested from a file where they are stored in
	// the "external"/"serialized" JSON-based format.
	// First read JSON into `txt` and ...
	txt, err := io.ReadAll(in)
	if err != nil {
		log.Fatalln(err)
	}
	// ... then deserialize JSON stored in `txt` into the content of `serialized`.
	var serialized Paths
	if err = json.Unmarshal(txt, &serialized); err != nil {
		log.Fatalln(err)
	}
	// Process the external-formatted `serialized` structure to create a list of
	// internal-formatted records describing the paths to be tested.
	records := processJSON(serialized)

	cnt := 0

	// Iterate over all found templates.
	dir, err := ts.ReadDir("templates")
	if err != nil {
		log.Fatalln(err)
	}
	for _, f := range dir {
		templ, err := ts.ReadFile("templates/" + f.Name())
		if err != nil {
			log.Fatalln(err)
		}

		// Generate the JSON test sub-string.
		t, err := template.New(f.Name()).Parse(string(templ))
		if err != nil {
			log.Fatalln(err)
		}
		for _, path := range records {
			for elem := path; elem != nil; {
				// Is this sub-path marked as one to skip.
				if !elem.Skip {
					buf := new(bytes.Buffer)
					if err = t.Execute(buf, elem); err != nil {
						log.Fatalln(err)
					}
					// Print out the template-generated content.
					out := os.Stdout
					if *outdir != "" {
						cnt = cnt + 1
						path := fmt.Sprintf("%s/gentest_%06d.json", filepath.Dir(*outdir), cnt)
						if out, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644); err != nil {
							log.Fatalln(err)
						}
					}
					defer out.Close()
					prettyJSON := new(bytes.Buffer)
					if err := json.Indent(prettyJSON, buf.Bytes(), "", "    "); err != nil {
						fmt.Println(buf.String())
						log.Fatalln(err)
					}
					fmt.Println(prettyJSON.String())
					if _, err := prettyJSON.WriteTo(out); err != nil {
						log.Fatalln(err)
					}
				}
				// Pick the next element in the list.
				if len(elem.Children) == 0 {
					break
				}
				elem = elem.Children[0]
			}
		}
	}
}
