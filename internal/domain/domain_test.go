package domain

import "testing"

func TestTopicValidation(t *testing.T) {
	for _, s := range []string{"", "../escape", "a/b", "a\\b", ".hidden", "a b"} {
		if ValidName(s) {
			t.Fatal(s)
		}
	}
	for _, s := range []string{"orders", "orders.retry", "events-raw_1"} {
		if !ValidName(s) {
			t.Fatal(s)
		}
	}
}
func TestStableHash(t *testing.T) {
	p := KeyPartition([]byte("customer-42"), 3)
	if p != 1 {
		t.Fatalf("stable FNV-1a result changed: %d", p)
	}
	for i := 0; i < 100; i++ {
		if KeyPartition([]byte("customer-42"), 3) != p {
			t.Fatal("unstable")
		}
	}
}

func TestPortableTopicNames(t *testing.T) {
	for _, name := range []string{"Orders", "orders.", "con", "con.retry", "prn", "aux", "nul", "com1", "com9.events", "lpt1", "lpt9.retry", "../bad"} {
		if ValidTopicName(name) {
			t.Fatalf("unsafe topic accepted: %q", name)
		}
	}
	for _, name := range []string{"orders", "orders.retry", "events-raw_1", "com0", "com10", "console", "x.con"} {
		if !ValidTopicName(name) {
			t.Fatalf("portable topic rejected: %q", name)
		}
	}
	if !ValidName("A") {
		t.Fatal("group/member names should retain their case-sensitive contract")
	}
}
