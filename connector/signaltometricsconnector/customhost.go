package signaltometricsconnector

import "go.opentelemetry.io/collector/component"

type customHost struct{}

func newCustomHost() component.Host {
	return &customHost{}
}

func (h *customHost) GetExtensions() map[component.ID]component.Component {
	// Init extension here and return that as required
	return nil
}

func (h *customHost) GetFactory(_ component.Kind, _ component.Type) component.Factory {
	return nil
}
