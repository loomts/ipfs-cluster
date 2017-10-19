package basic

import (
	"encoding/json"
	"testing"
)

var cfgJSON = []byte(`
{
      "check_interval": "15s"
}
`)

func TestLoadJSON(t *testing.T) {
	cfg := &Config{}
	err := cfg.LoadJSON(cfgJSON)
	if err != nil {
		t.Fatal(err)
	}

	j := &jsonConfig{}

	json.Unmarshal(cfgJSON, j)
	j.CheckInterval = "-10"
	tst, _ := json.Marshal(j)
	err = cfg.LoadJSON(tst)
	if err == nil {
		t.Error("expected error decoding check_interval")
	}
}

func TestToJSON(t *testing.T) {
	cfg := &Config{}
	cfg.LoadJSON(cfgJSON)
	_, err := cfg.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefault(t *testing.T) {
	cfg := &Config{}
	cfg.Default()
	if cfg.Validate() != nil {
		t.Fatal("error validating")
	}

	cfg.CheckInterval = 0
	if cfg.Validate() == nil {
		t.Fatal("expected error validating")
	}
}
