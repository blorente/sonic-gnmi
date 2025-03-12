package pathtransl

import (
	lvl "github.com/sonic-net/sonic-gnmi/gnmi_server/log"
	log "github.com/golang/glog"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// Defines interface for path translators.

// Type: Translator ID
type TranslID = int

const InvalidTranslID = -1

// Type: Translator Context.
type TranslatorCtx interface {
	GetTranslID() TranslID
}

// Defines Translator Interface.
type Translator interface {
	Name() string
	TranslGetRequest(req *gnmipb.GetRequest) (bool, TranslatorCtx)
	TranslGetResponse(resp *gnmipb.GetResponse, ctx TranslatorCtx)
	TranslSetRequest(req *gnmipb.SetRequest) (bool, TranslatorCtx)
	TranslSetResponse(resp *gnmipb.SetResponse, ctx TranslatorCtx)
	TranslSubscribeRequest(req *gnmipb.SubscribeRequest) (bool, TranslatorCtx)
	TranslSubscribeResponse(resp *gnmipb.SubscribeResponse, ctx TranslatorCtx)
}

// All registered path translators.
var translators []Translator = []Translator{}

// Registers a path translator. Returns the assigned id.
func register(transl Translator) int {
	translators = append(translators, transl)
	log.V(lvl.DEBUG).Info("Register Path Translator:", transl.Name(), "with id:", len(translators))
	return len(translators) - 1
}

// Returns true if id is a valid translator id.
func isValidTranslID(id TranslID) bool {
	if id <= InvalidTranslID || id >= len(translators) {
		return false
	}
	return true
}

// Returns the translator from translator context.
// Returns nil if the context is not valid.
func getTranslFromCtx(ctx TranslatorCtx) Translator {
	if ctx != nil && isValidTranslID(ctx.GetTranslID()) {
		return translators[ctx.GetTranslID()]
	}
	return nil
}

// Translates a GetRequest.
func TranslGetRequest(req *gnmipb.GetRequest) TranslatorCtx {
	for _, transl := range translators {
		translated, ctx := transl.TranslGetRequest(req)
		if translated {
			log.V(lvl.DEBUG).Infoln("Translator: ", transl.Name(), " translating get")
			return ctx
		}
	}
	return nil
}

// Translates a GetResponse.
func TranslGetResponse(resp *gnmipb.GetResponse, ctx TranslatorCtx) {
	transl := getTranslFromCtx(ctx)
	if transl != nil {
		transl.TranslGetResponse(resp, ctx)
	}
}

// Translates a SetRequest.
func TranslSetRequest(req *gnmipb.SetRequest) TranslatorCtx {
	for _, transl := range translators {
		translated, ctx := transl.TranslSetRequest(req)
		if translated {
			log.V(lvl.DEBUG).Infoln("Translator: ", transl.Name(), " translating set")
			return ctx
		}
	}
	return nil
}

// Translates a SetResponse.
func TranslSetResponse(resp *gnmipb.SetResponse, ctx TranslatorCtx) {
	transl := getTranslFromCtx(ctx)
	if transl != nil {
		transl.TranslSetResponse(resp, ctx)
	}
}

// Translates a SubscribeRequest.
func TranslSubscribeRequest(req *gnmipb.SubscribeRequest) TranslatorCtx {
	for _, transl := range translators {
		translated, ctx := transl.TranslSubscribeRequest(req)
		if translated {
			log.V(lvl.DEBUG).Infoln("Translator: ", transl.Name(), " translating sub")
			return ctx
		}
	}
	return nil
}

// Translates a SubscribeResponse.
func TranslSubscribeResponse(resp *gnmipb.SubscribeResponse, ctx TranslatorCtx) {
	transl := getTranslFromCtx(ctx)
	if transl != nil {
		transl.TranslSubscribeResponse(resp, ctx)
	}
}
