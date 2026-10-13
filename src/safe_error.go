package paxocache

type SafeError struct {
	Msg string
}

func (this SafeError) Error() string {
	return this.Msg;
}