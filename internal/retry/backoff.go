package retry

import "time"

// Delay saturates before doubling, avoiding integer/duration overflow.
func Delay(attempt int, base, maximum time.Duration) time.Duration {
	if base <= 0 || maximum <= 0 {
		return 0
	}
	if base >= maximum {
		return maximum
	}
	d := base
	for i := 1; i < attempt; i++ {
		if d >= maximum-d {
			return maximum
		}
		d *= 2
	}
	return d
}
