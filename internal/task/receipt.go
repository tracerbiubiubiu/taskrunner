package task

import (
	"encoding/json"
	"errors"
	"strings"
)

const (
	TicketEvidenceReportAction     = "ticket.evidence.report.generate"
	ActiveListDocumentUpdateAction = "activelist.document.update"
	ActiveListDocumentGetAction    = "activelist.document.get"
	ReceiptTaskIDPrefix            = "b3_"
)

// UsesReceiptCallback selects the actions with the structured completion contract.
func UsesReceiptCallback(action string) bool {
	switch action {
	case TicketEvidenceReportAction, ActiveListDocumentUpdateAction, ActiveListDocumentGetAction:
		return true
	default:
		return false
	}
}

// ReceiptOperationID reads the exact operation_id from the frozen parameters.
func ReceiptOperationID(params []byte) (string, error) {
	var fields map[string]json.RawMessage
	var id string
	if json.Unmarshal(params, &fields) != nil || json.Unmarshal(fields["operation_id"], &id) != nil || strings.TrimSpace(id) == "" {
		return "", errors.New("params.operation_id 须为非空字符串")
	}
	return id, nil
}
