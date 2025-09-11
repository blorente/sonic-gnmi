package pathtransl

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	transformer "github.com/Azure/sonic-mgmt-common/translib/transformer"
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
)

type ocPathType int

const (
	ptInvalidPath  = iota // Invalid path
	ptOCPathPrefix        // An openconfig path but with no container name
	ptOCPath              // An openconfig path
	ptNonOCPath           // Not an openconfig path
)

const (
	setDeleteOp  = 0
	setReplaceOp = 1
	setUpdateOp  = 2
	numSetOps    = 3
)

type ocTranslator struct {
	name string
}

// Context for SetRequest Operations.
type ocSetOpCtx struct {
	delFullOrigin  bool
	updFullOrigin  bool
	replFullOrigin bool
}

type ocTranslCtx struct {
	useOrigin  bool            // if origin is used in original request
	prefixType ocPathType      // prefix path of original request
	origPrefix *gnmipb.Path    // copy of original prefix
	fullOrigin bool            // true if the request is for a single root, for GET/SUBSCRIBE
	encoding   gnmipb.Encoding // request encoding
	setOpCtx   *ocSetOpCtx     // context for SetRequest Ops
}

const openconfigOrigin string = "openconfig"

// List of all OC modules and their top container, "openconfig-module:container".
var AllOCModules = []string{}

// Map from top container name to module name.
var containerToModuleMap map[string]string

// Translator id for openconfig translator
var ocTranslID TranslID

func init() {
	// Registers openconfig translator.
	ocTransl := ocTranslator{name: openconfigOrigin}
	ocTranslID = register(&ocTransl)
}

var once sync.Once

var spaces = [...]string{" ", "\n", "\t"}

// Removes all spaces (' ', '\n', '\t') from a string.
func removeSpaces(str string) string {
	for _, spc := range spaces {
		str = strings.ReplaceAll(str, spc, "")
	}

	return str
}

// Marshals a protobuf message to text string. Removes all spaces if packed is true.
func marshalProtoMessage(msg proto.Message, packed bool) string {
	str := proto.MarshalTextString(msg)
	if packed {
		str = removeSpaces(str)
	}
	return str
}

func marshalProtoMessagePacked(msg proto.Message) string {
	return marshalProtoMessage(msg, true)
}

func newOCTranslCtx() *ocTranslCtx {
	// Initialize our translation state if needed.
	onceFunc := func() {
		yangMods := transformer.OcYangModuleGet()
		AllOCModules = make([]string, len(yangMods))
		containerToModuleMap = make(map[string]string)
		i := 0
		for k, v := range yangMods {
			AllOCModules[i] = k + ":" + v
			i++
			containerToModuleMap[v], _ = strings.CutPrefix(k, "openconfig-")
		}
	}
	once.Do(onceFunc)

	return &ocTranslCtx{useOrigin: false, prefixType: ptInvalidPath,
		origPrefix: nil, fullOrigin: false}
}

// Returns ocPathType of path.
func getPathType(path *gnmipb.Path) ocPathType {
	if path == nil {
		return ptInvalidPath
	}

	origin := path.GetOrigin()
	elem := path.GetElem()

	if origin != "" {
		if origin != openconfigOrigin {
			return ptNonOCPath
		}

		if elem != nil {
			return ptOCPath
		}
		return ptOCPathPrefix
	}

	if elem == nil || len(elem) == 0 || elem[0].GetName() == "" {
		return ptInvalidPath
	}
	if elem[0].GetName() == openconfigOrigin {
		if len(elem) == 1 {
			return ptOCPathPrefix
		}
		return ptOCPath
	}
	return ptNonOCPath
}

// Checks if path is a single root path.
//
//	hasPrefix: true if the request has a OC prefix.
//
// Returns:
//
//	singleRoot: true if the path is a single root path.
//	useOrigin: true if the path is a single root path and origin is "openconfig".
func isPathSingleRoot(path *gnmipb.Path, hasPrefix bool) (singleRoot, useOrigin bool) {
	if hasPrefix {
		if path == nil || path.GetElem() == nil || len(path.GetElem()) == 0 {
			return true, false
		}
		return false, false
	}

	switch getPathType(path) {
	case ptOCPathPrefix:
		if path.GetOrigin() != "" {
			return true, false
		}
		return true, true
	}

	return false, false
}

// If path has Origin, removes the origin, put the origin as the first
// element of the Elem and returns true.
func ocToUMFFixOrigin(path *gnmipb.Path) bool {
	origin := path.GetOrigin()
	if origin != "" {
		path.Origin = ""
		path.Elem = append([]*gnmipb.PathElem{&gnmipb.PathElem{Name: origin}}, path.Elem...)
		return true
	}
	return false
}

// Translates a path from openconfig format to UMF format.
//
//	path               path to be translated
//	hasPrefix          if there is prefix for the path
//
// Returns
//
//	translated         true if the path is translated
//	useOrigin          true if the path has Origin
func ocToUMFPath(path *gnmipb.Path, hasPrefix bool) (translated, useOrigin bool) {
	if !hasPrefix {
		switch getPathType(path) {
		case ptNonOCPath:
			return false, false

		case ptInvalidPath:
			return false, false

		case ptOCPathPrefix:
			return false, false

		case ptOCPath:
		}

		// Fixes origin first if there is.
		useOrigin = ocToUMFFixOrigin(path)

		// Extracts the module name and container name.
		elem := path.GetElem()
		cName := elem[1].GetName()
		mName, hasKey := containerToModuleMap[cName]
		if !hasKey {
			mName = cName
		}
		// Converts the first element to UMF format: orign-module:container
		elem[1].Name = elem[0].GetName() + "-" + mName + ":" + cName
		path.Elem = elem[1:]

		return true, useOrigin
	}

	elem := path.GetElem()
	if elem == nil || len(elem) == 0 {
		return false, false
	}

	// Extracts the module name and container name.
	cName := elem[0].GetName()
	mName, hasKey := containerToModuleMap[cName]
	if !hasKey {
		mName = cName
	}
	// Converts the first element to UMF format: openconfig-module:container.
	elem[0].Name = openconfigOrigin + "-" + mName + ":" + cName

	return true, false
}

// Translates a prefix from openconfig to UMF format. Returns:
//
//	prefixFixed      true if the prefix is translated
//	needFixPath      true if the paths using this prefix need to be translated as well.
//	                 false no need.
func ocToUMFPrefix(prefix *gnmipb.Path, ctx *ocTranslCtx) (prefixFixed, needFixPath bool) {
	ctx.useOrigin = false

	switch getPathType(prefix) {
	case ptNonOCPath:
		return false, false

	case ptOCPathPrefix:
		ctx.useOrigin = ocToUMFFixOrigin(prefix)
		ctx.prefixType = ptOCPathPrefix
		prefix.Elem = nil
		return true, true

	case ptOCPath:
		// Fixes prefix and it is all good.
		_, ctx.useOrigin = ocToUMFPath(prefix, false)
		prefixFixed = true
		ctx.prefixType = ptOCPath
		return true, false

	case ptInvalidPath:
		return false, true
	}
	return false, false
}

// Translates a path from UMF format to openconfig format.
// Returns true if translated.
func umfToOCPath(path *gnmipb.Path, ctx *ocTranslCtx) bool {
	if path == nil {
		return false
	}

	elem := path.GetElem()
	if elem == nil || len(elem) == 0 {
		return false
	}

	// Remove module prefixes on all but the first element
	for i, e := range elem {
		if i == 0 {
			continue
		}
		_, name, found := strings.Cut(e.Name, ":")
		if found {
			e.Name = name
		}
	}

	// Retrieves module name and top container name if there is.
	uName := elem[0].GetName()
	uNameElem := strings.Split(uName, ":")
	if len(uNameElem) <= 1 {
		return false
	}
	var cName = uNameElem[1]

	// Sets the container name as the first element.
	path.Elem[0].Name = cName
	if ctx.prefixType != ptInvalidPath {
		// Has openconfig prefix, it is all good.
		return true
	}

	// No prefix, extracts the origin and put it in the translated path.
	oName := strings.Split(uNameElem[0], "-")[0]
	if ctx.useOrigin {
		path.Origin = oName
	} else {
		path.Elem = append([]*gnmipb.PathElem{&gnmipb.PathElem{Name: oName}}, elem...)
	}
	return true
}

// Translates a prefix from UMF format to openconfig format.
// Returns:
//
//	translated: prefix is valid and translated.
//	hasPath:    prefix is valid and has path element.
func umfToOCPrefix(prefix *gnmipb.Path, ctx *ocTranslCtx) (translated, hasPath bool) {
	if prefix == nil {
		return false, false
	}
	if prefix.Origin == "" {
		prefix.Origin = ctx.origPrefix.Origin
	}

	elem := prefix.GetElem()
	if elem == nil || len(elem) == 0 {
		if ctx.prefixType == ptInvalidPath || ctx.prefixType == ptNonOCPath {
			return false, false
		}
		prefix.Origin = openconfigOrigin
		ctx.prefixType = ptOCPathPrefix
		return true, false
	}

	// Retrieves module name and top container name if there is.
	uName := elem[0].GetName()
	uNameElem := strings.Split(uName, ":")
	if len(uNameElem) <= 1 {
		ctx.prefixType = ptInvalidPath
		log.V(lvl.WARNING).Info("Invalid UMF prefix format:" + uName)
		return false, false
	}
	var cName = uNameElem[1]

	// Extracts the origin
	omNames := strings.Split(uNameElem[0], "-")
	if len(omNames) < 2 {
		ctx.prefixType = ptInvalidPath
		log.V(lvl.WARNING).Info("Invalid UMF prefix format (no module name):" + uName)
		return false, false
	}

	// Replaces elem[0] with top container name.
	elem[0].Name = cName

	// Set origin according to the format of the request.
	oName := omNames[0]
	if ctx.useOrigin {
		prefix.Origin = oName
	} else {
		prefix.Elem = append([]*gnmipb.PathElem{&gnmipb.PathElem{Name: oName}}, elem...)
	}
	ctx.prefixType = ptOCPath

	// Remove any module names from subsequent path elements.
	for i, e := range elem {
		if i == 0 {
			continue
		}
		_, name, found := strings.Cut(e.Name, ":")
		if found {
			e.Name = name
		}
	}
	return true, true
}

// Translates an notification from UMF format to openconfig format.
func umfToOCNotification(ntfc *gnmipb.Notification, ctx *ocTranslCtx) {
	// Response prefix may change in new UMF code drop.
	// We need to translate the prefix all the time.
	prefix := ntfc.GetPrefix()
	if prefix == nil && ctx.prefixType != ptNonOCPath {
		// This only happens for PROTO encoding of top level nodes.
		// If original request has prefix, generates a prefix for the response.
		prefix = &gnmipb.Path{Target: ctx.origPrefix.Target}
		if ctx.useOrigin {
			prefix.Origin = openconfigOrigin
		} else {
			prefix.Elem = []*gnmipb.PathElem{&gnmipb.PathElem{Name: openconfigOrigin}}
		}
		ntfc.Prefix = prefix
		ctx.prefixType = ptOCPathPrefix
	} else {
		umfToOCPrefix(prefix, ctx)
	}

	// Translates all the updated paths.
	for _, upd := range ntfc.GetUpdate() {
		umfToOCPath(upd.GetPath(), ctx)
	}
	// Translates all the deleted paths.
	for _, delPath := range ntfc.GetDelete() {
		umfToOCPath(delPath, ctx)
	}
}

func fillAllOCPaths(paths *[]*gnmipb.Path) {
	for _, mName := range AllOCModules {
		mPath := gnmipb.Path{Elem: []*gnmipb.PathElem{&gnmipb.PathElem{Name: mName}}}
		*paths = append(*paths, &mPath)
	}
}

/* Given a json input, returns the top level keys as an array of strings
 * preserving the order of those keys from the input json. */
func jsonKeys(jsonVal []byte) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(jsonVal))
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if token != json.Delim('{') {
		return nil, errors.New("Malformed JSON input, expected \"{\"")
	}
	var rv []string
	endSignal := errors.New("invalid end of array or object")
	for {
		token, err = dec.Token()
		if err != nil {
			return nil, err
		}
		if token == json.Delim('}') {
			return rv, nil
		}
		rv = append(rv, token.(string))
		err = skipJsonValue(dec, endSignal)
		if err != nil {
			return nil, err
		}
	}
}

// Helper function for jsonKeys implementation
func skipJsonValue(dec *json.Decoder, endSignal error) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('['), json.Delim('{'):
		for {
			if err := skipJsonValue(dec, endSignal); err != nil {
				if err == endSignal {
					break
				}
				return err
			}
		}
	case json.Delim(']'), json.Delim('}'):
		return endSignal
	}
	return nil
}

/* Convert an "OpenConfig style" name, e.g. interfaces, to "UMF style", e.g.
 * "openconfig-interfaces:interfaces". */
func translateNameOCToUMF(name string) string {
	if !strings.Contains(name, ":") {
		mName := name
		mName, _ = containerToModuleMap[name]
		name = openconfigOrigin + "-" + mName + ":" + name
	}
	return name
}

// Splits a single root (full origin) update into multiple UMF updates.
func ocToUMFSingleRootUpdate(update *gnmipb.Update) *[]*gnmipb.Update {
	var splittedUpds []*gnmipb.Update

	val := update.GetVal()

	if val == nil {
		// Generates splitted updates.
		splittedUpds = make([]*gnmipb.Update, len(AllOCModules))
		for i, mName := range AllOCModules {
			mPath := gnmipb.Path{Elem: []*gnmipb.PathElem{&gnmipb.PathElem{Name: mName}}}
			splittedUpds[i] = &gnmipb.Update{Path: &mPath, Val: nil, Duplicates: update.GetDuplicates()}
		}
		return &splittedUpds
	}

	// Only json format is support. There is no single structured proto update
	// being defined yet. There is no requirement for single root proto as well.
	jsonVal := val.GetJsonVal()
	encoding := gnmipb.Encoding_JSON
	if jsonVal == nil {
		jsonVal = val.GetJsonIetfVal()
		encoding = gnmipb.Encoding_JSON_IETF
	}
	if jsonVal == nil {
		log.V(lvl.WARNING).Info("Only json format is supported for single root update.")
		return nil
	}

	// Get the order of the top level requests from the json
	orderedKeys, err := jsonKeys(jsonVal)
	if err != nil {
		log.V(lvl.ERROR).Infof("Error %v parsing json", err)
		return nil
	}

	// Translate the top level names and create a map from name to update number
	// to easily map a name to its position.
	keyToPos := make(map[string]int)
	for i, name := range orderedKeys {
		translatedName := translateNameOCToUMF(name)
		keyToPos[translatedName] = i
	}

	splittedUpds = make([]*gnmipb.Update, len(orderedKeys))

	// Process the json once more; for each module translate the name, create an
	// update, and insert it into the return array at the correct position.
	var result map[string]interface{}
	err = json.Unmarshal(jsonVal, &result)
	if err != nil {
		log.V(lvl.ERROR).Infof("Error %v unmarshaling json", err)
		return nil
	}
	for k, v := range result {
		k = translateNameOCToUMF(k)
		valJSON, err := json.Marshal(v)
		if err != nil {
			log.V(lvl.ERROR).Infof("Error %v marshaling json for %s", err, k)
			return nil
		}
		mPath := gnmipb.Path{Elem: []*gnmipb.PathElem{&gnmipb.PathElem{Name: k}}}
		jVal := append(([]byte)("{\""+k+"\":"), valJSON...)
		jVal = append(jVal, ([]byte)("}")...)
		var typedVal *gnmipb.TypedValue
		if encoding == gnmipb.Encoding_JSON {
			typedVal = &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonVal{JsonVal: jVal}}
		} else {
			typedVal = &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: jVal}}
		}
		upd := gnmipb.Update{Path: &mPath, Val: typedVal, Duplicates: update.GetDuplicates()}
		splittedUpds[keyToPos[k]] = &upd
	}

	return &splittedUpds
}

// Combine all notifications into one notification and put it in notifications[0].
// Only Json and JsonIetf encodings are supported.
func combineNotifications(encoding gnmipb.Encoding, notifications []*gnmipb.Notification) bool {
	if encoding != gnmipb.Encoding_JSON && encoding != gnmipb.Encoding_JSON_IETF {
		log.V(lvl.WARNING).Infoln("Encdoing not supported.")
		return false
	}

	// Json header for the value
	finalJVal := []byte("{")
	updated := false
	for _, notification := range notifications {
		for _, update := range notification.GetUpdate() {
			val := update.GetVal()
			if val == nil {
				continue
			}
			var jVal []byte
			if encoding == gnmipb.Encoding_JSON {
				jVal = val.GetJsonVal()
			} else {
				// Encoding_JSON_IETF
				jVal = val.GetJsonIetfVal()
			}
			// UMF will return {} for no content, we will filter it out based on the size.
			if len(jVal) < 4 {
				continue
			}
			jVal[len(jVal)-1] = ','
			finalJVal = append(finalJVal, jVal[1:]...)
			updated = true
		}
	}

	if !updated {
		return false
	}

	// closes the json header
	finalJVal[len(finalJVal)-1] = '}'

	var finalUpdate gnmipb.Update
	if encoding == gnmipb.Encoding_JSON {
		finalUpdate.Val = &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonVal{JsonVal: finalJVal}}
	} else {
		finalUpdate.Val = &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: finalJVal}}
	}

	notifications[0].Update = []*gnmipb.Update{&finalUpdate}

	return true
}

func (c *ocTranslCtx) GetTranslID() TranslID {
	return ocTranslID
}

func (ot *ocTranslator) Name() string {
	return ot.name
}

// All Request translation below:
// 1. Supports Origin
// 2. Translates /openconfig/top_container to /openconfig-modulename:top_container/
// 3. Support single root(origin) GET (TODO)

// Translates a GetRequest from Openconfig format to UMF format
func (ot *ocTranslator) TranslGetRequest(req *gnmipb.GetRequest) (bool, TranslatorCtx) {
	prefix := req.GetPrefix()
	ctx := newOCTranslCtx()
	ctx.origPrefix, _ = proto.Clone(prefix).(*gnmipb.Path)
	ctx.encoding = req.GetEncoding()

	prefixFixed, needFixPath := ocToUMFPrefix(prefix, ctx)

	if !needFixPath {
		if prefixFixed {
			ctx.prefixType = ptOCPath
			if log.V(lvl.DEBUG) {
				log.Infof("Translated GetRequest(prefix): %.1000s", marshalProtoMessage(req, false /*packed=*/))
			}
		}
		return prefixFixed, TranslatorCtx(ctx)
	}

	if prefixFixed && (req.GetPath() == nil || len(req.GetPath()) == 0) {
		ctx.fullOrigin = true
		fillAllOCPaths(&req.Path)
		if log.V(lvl.DEBUG) {
			log.Infof("Translated GetRequest(full origin): %.1000s", marshalProtoMessage(req, false /*packed=*/))
		}
		return true, TranslatorCtx(ctx)
	}

	translated := false

	for _, path := range req.GetPath() {
		tret, oret := ocToUMFPath(path, prefixFixed)
		translated = translated || tret
		ctx.useOrigin = ctx.useOrigin || oret
	}

	if translated {
		if log.V(lvl.DEBUG) {
			log.Infof("Translated GetRequest(path): %.1000s", marshalProtoMessage(req, false /*packed=*/))
		}
	}

	return translated, TranslatorCtx(ctx)
}

// Translates GetResponse from UMF format to openconfig format.
func (ot *ocTranslator) TranslGetResponse(resp *gnmipb.GetResponse, ctx TranslatorCtx) {
	ocCtx := ctx.(*ocTranslCtx)

	if log.V(lvl.DEBUG) {
		log.Infof("Translating GetResponse: %.1000s", marshalProtoMessage(resp, false /*packed=*/))
	}

	if resp.GetNotification() == nil {
		log.V(lvl.DEBUG).Infoln("No notifications.")
		return
	}

	for _, notification := range resp.GetNotification() {
		umfToOCNotification(notification, ocCtx)
	}

	encoding := ocCtx.encoding
	if !ocCtx.fullOrigin {
		if log.V(lvl.DEBUG) {
			log.Infof("Translated GetResponse: %.1000s", marshalProtoMessage(resp, false /*packed=*/))
		}
		return
	}

	if encoding != gnmipb.Encoding_JSON && encoding != gnmipb.Encoding_JSON_IETF {
		if log.V(lvl.DEBUG) {
			log.Infof("Translated GetResponse: notifications not combined, only Json encodings are supported, encoding=%v %.1000s.", encoding, marshalProtoMessage(resp, false /*packed=*/))
		}
		return
	}

	if !combineNotifications(encoding, resp.GetNotification()) {
		return
	}

	resp.Notification = resp.Notification[:1]
	if log.V(lvl.DEBUG) {
		log.Infof("Translated GetResponse (full origin): %.1000s", marshalProtoMessage(resp, false /*packed=*/))
	}
	return
}

// Translates SetRequest from openconfig format to UMF format.
func (ot *ocTranslator) TranslSetRequest(req *gnmipb.SetRequest) (bool, TranslatorCtx) {

	prefix := req.GetPrefix()
	ctx := newOCTranslCtx()
	ctx.origPrefix, _ = proto.Clone(prefix).(*gnmipb.Path)
	ctx.setOpCtx = &ocSetOpCtx{}
	opCtx := ctx.setOpCtx

	prefixFixed, needFixPath := ocToUMFPrefix(prefix, ctx)

	if !needFixPath {
		if prefixFixed {
			if log.V(lvl.DEBUG) {
				log.Infof("Translated SetRequest(prefix): %.1000s", marshalProtoMessage(req, false /*packed=*/))
			}
		}
		return prefixFixed, ctx
	}

	translated := false

	for _, path := range req.GetDelete() {
		sret, oret := isPathSingleRoot(path, prefixFixed)
		if sret {
			// a single root path. Need to translate it to multiple paths.
			paths := []*gnmipb.Path{}
			fillAllOCPaths(&paths)
			// Records the splitted del context.
			opCtx.delFullOrigin = true
			req.Delete = paths
			translated = true
			// We don't support mixed single root delete and non single root delete.
			break
		}

		// Not a single root path, translates the path.
		tret, oret := ocToUMFPath(path, prefixFixed)
		translated = translated || tret
		ctx.useOrigin = ctx.useOrigin || oret
	}

	for _, upd := range req.GetUpdate() {
		// if the update path is single root, then we need to translate it into multiple updates.
		sret, oret := isPathSingleRoot(upd.GetPath(), prefixFixed)
		if sret {
			if upds := ocToUMFSingleRootUpdate(upd); upds != nil {
				// Records the splitted update context.
				opCtx.updFullOrigin = true
				req.Update = *upds
				translated = true
				// We don't support mixed single-root and non single-root update.
				break
			}
		}

		// Not a single root path, translates the path.
		tret, oret := ocToUMFPath(upd.GetPath(), prefixFixed)
		translated = translated || tret
		ctx.useOrigin = ctx.useOrigin || oret
	}

	for _, repl := range req.GetReplace() {
		// if the update path is single root, then we need to translate it into multiple updates.
		sret, oret := isPathSingleRoot(repl.GetPath(), prefixFixed)
		if sret {
			if repls := ocToUMFSingleRootUpdate(repl); repls != nil {
				// Records the splitted update context.
				opCtx.replFullOrigin = true
				req.Replace = *repls
				translated = true
				// We don't support mixed single-root and non single-root replace.
				break
			}
		}

		// Not a single root path, translates the path.
		tret, oret := ocToUMFPath(repl.GetPath(), prefixFixed)
		translated = translated || tret
		ctx.useOrigin = ctx.useOrigin || oret
	}

	if translated {
		if log.V(lvl.DEBUG) {
			log.Infof("Translated SetRequest: %.1000s", marshalProtoMessage(req, false /*packed=*/))
		}
	} else {
		req.Prefix = ctx.origPrefix
	}

	return translated, ctx
}

// Translates SetResponse from UMF format to openconfig format.
func (ot *ocTranslator) TranslSetResponse(resp *gnmipb.SetResponse, ctx TranslatorCtx) {
	ocCtx := ctx.(*ocTranslCtx)

	if log.V(lvl.DEBUG) {
		log.Infof("Translating SetResponse: %.1000s", marshalProtoMessage(resp, false /*packed=*/))
	}

	hasPrefix, hasPath := umfToOCPrefix(resp.GetPrefix(), ocCtx)
	if hasPath {
		if log.V(lvl.DEBUG) {
			log.Infof("Translated SetResponse(prefix): %.1000s", marshalProtoMessage(resp, false /*packed=*/))
		}
		return
	}

	var translResps []*gnmipb.UpdateResult

	if ocCtx.setOpCtx.delFullOrigin {
		resp := &gnmipb.UpdateResult{Op: gnmipb.UpdateResult_DELETE}
		if !hasPrefix {
			resp.Path = &gnmipb.Path{Origin: openconfigOrigin}
		}
		translResps = append(translResps, resp)
	}

	if ocCtx.setOpCtx.replFullOrigin {
		resp := &gnmipb.UpdateResult{Op: gnmipb.UpdateResult_REPLACE}
		if !hasPrefix {
			resp.Path = &gnmipb.Path{Origin: openconfigOrigin}
		}
		translResps = append(translResps, resp)
	}

	if ocCtx.setOpCtx.updFullOrigin {
		resp := &gnmipb.UpdateResult{Op: gnmipb.UpdateResult_UPDATE}
		if !hasPrefix {
			resp.Path = &gnmipb.Path{Origin: openconfigOrigin}
		}
		translResps = append(translResps, resp)
	}

	for _, updResp := range resp.GetResponse() {
		// We don't support mix of single root op and non single root op.
		// So if a single root op is set, all other same individual ops are ignored.
		switch updResp.Op {
		case gnmipb.UpdateResult_DELETE:
			if ocCtx.setOpCtx.delFullOrigin {
				continue
			}

		case gnmipb.UpdateResult_REPLACE:
			if ocCtx.setOpCtx.replFullOrigin {
				continue
			}

		case gnmipb.UpdateResult_UPDATE:
			if ocCtx.setOpCtx.updFullOrigin {
				continue
			}
		}

		umfToOCPath(updResp.GetPath(), ocCtx)
		translResps = append(translResps, updResp)
	}
	resp.Response = translResps

	if log.V(lvl.DEBUG) {
		log.Infof("Translated SetResponse: %.1000s", marshalProtoMessage(resp, false /*packed=*/))
	}
}

// Translates SubscribeRequest from openconfig format to UMF format.
func (ot *ocTranslator) TranslSubscribeRequest(req *gnmipb.SubscribeRequest) (bool, TranslatorCtx) {
	sublist := req.GetSubscribe()
	if sublist == nil {
		return false, nil
	}

	if log.V(lvl.DEBUG) {
		log.Infof("Translating SubscribeRequest: %.1000s", marshalProtoMessage(req, false /*packed=*/))
	}
	prefix := sublist.GetPrefix()
	ctx := newOCTranslCtx()
	ctx.origPrefix, _ = proto.Clone(prefix).(*gnmipb.Path)

	prefixFixed, needFixPath := ocToUMFPrefix(prefix, ctx)

	if !needFixPath {
		if prefixFixed {
			if log.V(lvl.DEBUG) {
				log.Infof("Translated SubscribeRequest(prefix): %.1000s", marshalProtoMessage(req, false /*packed=*/))
			}
		}
		return prefixFixed, ctx
	}

	// Checks if it is a single root subscribe. There could be one subscription
	// with no path, but with other subscribe parameters.
	singleRootSub := false
	origSubs := sublist.GetSubscription()
	if len(origSubs) > 0 {
		singleRootSub, _ = isPathSingleRoot(origSubs[0].GetPath(), prefixFixed)
	}
	if prefixFixed && (len(origSubs) == 0 || singleRootSub) {
		ctx.fullOrigin = true
		var translSubs []*gnmipb.Subscription
		for _, mName := range AllOCModules {
			var mSub *gnmipb.Subscription

			mPath := gnmipb.Path{Elem: []*gnmipb.PathElem{&gnmipb.PathElem{Name: mName}}}
			if len(origSubs) == 0 {
				mSub = &gnmipb.Subscription{Path: &mPath}
			} else {
				// Preserves all subscription parameters.
				mSub = proto.Clone(origSubs[0]).(*gnmipb.Subscription)
				mSub.Path = &mPath
			}
			translSubs = append(translSubs, mSub)
		}
		sublist.Subscription = translSubs
		if log.V(lvl.DEBUG) {
			log.Infof("Translated SubscribeRequest(full origin): %.1000s", marshalProtoMessage(req, false /*packed=*/))
		}
		return true, TranslatorCtx(ctx)
	}

	translated := false

	for _, sub := range sublist.GetSubscription() {
		tret, oret := ocToUMFPath(sub.GetPath(), prefixFixed)
		translated = translated || tret
		ctx.useOrigin = ctx.useOrigin || oret
	}

	if translated {
		if log.V(lvl.DEBUG) {
			log.Infof("Translated SubscribeRequest: %.1000s", marshalProtoMessage(req, false /*packed=*/))
		}
	}
	return translated, ctx
}

// Translates SubscribeResponse from UMF format to openconfig format.
func (ot *ocTranslator) TranslSubscribeResponse(resp *gnmipb.SubscribeResponse, ctx TranslatorCtx) {
	ocCtx := ctx.(*ocTranslCtx)

	updn := resp.GetUpdate()

	if updn == nil {
		return
	}

	if log.V(lvl.DEBUG) {
		log.Infof("Translating SubscribeResponse: %.1000s", marshalProtoMessage(resp, false /*packed=*/))
	}
	umfToOCNotification(updn, ocCtx)
	if log.V(lvl.DEBUG) {
		log.Infof("Translated SubscribeResponse: %.1000s", marshalProtoMessage(resp, false /*packed=*/))
	}
}
