//go:build race

package daemon

// raceOn: the race detector slows code down many times, so timing bounds
// do not hold.
const raceOn = true
