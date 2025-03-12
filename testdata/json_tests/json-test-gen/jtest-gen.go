package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"strconv"
	"text/template"
)

var (
	//go:embed template.tmpl
	templ   string
	inname  = flag.String("in", "", "path to the input file")
	outname = flag.String("out", "", "path to the output file")
)

type Element struct {
	Name     string
	Type     string
	Val      string
	Children []Element
}

func processJSON(name string, data interface{}) Element {
	switch v := data.(type) {
	case map[string]interface{}:
		chldn := []Element{}
		for key, val := range v {
			chldn = append(chldn, processJSON(key, val))
		}
		return Element{
			Name:     name,
			Type:     "object",
			Val:      "",
			Children: chldn,
		}
	case []interface{}:
		chldn := []Element{}
		for idx, val := range v {
			chldn = append(chldn, processJSON(strconv.FormatInt(int64(idx), 10), val))
		}
		return Element{
			Name:     name,
			Type:     "array",
			Val:      "",
			Children: chldn,
		}
	case float64:
		if v == float64(int64(v)) {
			return Element{Name: name, Type: "integer", Val: strconv.FormatInt(int64(v), 10)}
		}
		return Element{Name: name, Type: "float", Val: strconv.FormatFloat(v, 'f', -1, 64)}
	case string:
		return Element{Name: name, Type: "string", Val: v}
	case bool:
		return Element{Name: name, Type: "bool", Val: strconv.FormatBool(v)}
	}
	log.Fatalf("Unknown element `%v` type", data)
	return Element{}
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

	// Read the message to be used as input.
	msg, err := io.ReadAll(in)
	if err != nil {
		log.Fatalln(err)
	}
	var data interface{}
	if err = json.Unmarshal(msg, &data); err != nil {
		log.Fatalln(err)
	}

	// Parse the input JSON-encoded message.
	root := processJSON("root", data)

	// Generate the JSON test sub-string.
	t, err := template.New("test").Parse(templ)
	if err != nil {
		log.Fatalln(err)
	}

	// Print out the template-generated content.
	out := os.Stdout
	if *outname != "" {
		if out, err = os.OpenFile(*outname, 0x666, os.ModeAppend); err != nil {
			log.Fatalln(err)
		}
	}
	defer out.Close()
	if err = t.Execute(out, root); err != nil {
		log.Fatalln(err)
	}
}
