package glock

// SliceIQ creates an infinite buffered channel taking input on
// in and sending output to next.  SliceIQ should be run in its
// own goroutine.

type SliceIQ struct {
	poolIn  chan *connection
	poolOut chan *connection
	pending []*connection
}

func NewSliceIQ() *SliceIQ {
	siq := SliceIQ{}
	siq.poolIn = make(chan *connection)
	siq.poolOut = make(chan *connection)
	// pending events (this is the "infinite" part)
	//pending = []*connection{}
	return &siq
}

func (siq *SliceIQ) Size() int {
	return len(siq.pending)
}

func (siq *SliceIQ) Start() {
	in := siq.poolIn
	next := siq.poolOut
	defer close(next)

recv:
	for {
		// Ensure that pending always has values so the select can
		// multiplex between the receiver and sender properly
		if len(siq.pending) == 0 {
			v, ok := <-in
			if !ok {
				// in is closed, flush values
				break
			}

			// We now have something to send
			siq.pending = append(siq.pending, v)
		}

		select {
		// Queue incoming values
		case v, ok := <-in:
			if !ok {
				// in is closed, flush values
				break recv
			}
			siq.pending = append(siq.pending, v)

		// Send queued values
		case next <- siq.pending[0]:
			siq.pending = siq.pending[1:]
		}
	}

	// After in is closed, we may still have events to send
	for _, v := range siq.pending {
		next <- v
	}
}

func (siq *SliceIQ) Close() {
	close(siq.poolIn)
	close(siq.poolOut)
}
