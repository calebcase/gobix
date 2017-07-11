package main

import (
	"fmt"
	"io"
	"os"
	"sync"
)

const high_bit = 1 << 7

func all(flags []bool) bool {
	for _, flag := range flags {
		if !flag {
			return false
		}
	}
	return true
}

func min(values ...int) int {
	minimum := values[0]
	for _, value := range values {
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}

type Flow interface {
	// Attach should connect to the input for the flow and begin reading
	// data from it. It should run the processing of the input and the
	// production to the output channel in a Go function. When all the
	// input has been read then this output channel should be closed. The
	// buffer argument determines the buffering size of the channel. Size
	// determines the number of bytes desired in the byte array. It should
	// be considered a hint, but not a hard requirement.
	Attach(buffer, size int) (output <-chan []byte)
}

type AttachableFlow struct {
	mutex    sync.Mutex
	attached bool
	outs     []chan<- []byte
}

func (a *AttachableFlow) AddOut(out chan<- []byte) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.outs = append(a.outs, out)
}

func (a *AttachableFlow) Run(flow func()) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if !a.attached {
		go flow()
		a.attached = true
	}
}

func (a *AttachableFlow) Send(bytes []byte) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Write to all in parallel to avoid any one write blocking completing
	// the rest. The intention here is to ensure output is always being
	// pumped to our readers.
	var wg sync.WaitGroup
	for _, out := range a.outs {
		wg.Add(1)
		go func(out chan<- []byte) {
			defer wg.Done()
			out <- bytes
		}(out)
	}
	wg.Wait()
}

func (a *AttachableFlow) Finish() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Close out all our output streams.
	for _, out := range a.outs {
		close(out)
	}

	a.outs = nil
}

type ReducingFlow struct {
	mutex sync.Mutex

	// We will need to track the doneness of our input streams. If
	// one is done, then we won't read from it anymore.
	done []bool

	ins []<-chan []byte
}

func (r *ReducingFlow) AddIn(in <-chan []byte) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.done = append(r.done, false)
	r.ins = append(r.ins, in)
}

func (r *ReducingFlow) Receive(flow func(inputs ...[]byte)) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var inputs [][]byte
	for range r.ins {
		inputs = append(inputs, nil)
	}

	var wg sync.WaitGroup
	for x, in := range r.ins {
		if !r.done[x] {
			wg.Add(1)
			go func(x int, in <-chan []byte) {
				defer wg.Done()

				input, ok := <-in
				if ok {
					inputs[x] = input
				} else {
					r.done[x] = true
				}
			}(x, in)
		}
	}
	wg.Wait()

	flow(inputs...)
}

// Attach to the flow and discard all the messages in the channel until it is
// closed. Useful for draining flows that don't output anything on their
// channel and just hold it open while they do side-effects (e.g. WriterFlow).
func DrainFlow(f Flow) {
	for range f.Attach(8, 256) {
	}
}

type StringFlow struct {
	AttachableFlow

	str   string
	bytes []byte
}

func FromString(str string) *StringFlow {
	s := &StringFlow{
		str:   str,
		bytes: []byte(str),
	}

	return s
}

func (s *StringFlow) Attach(buffer, size int) <-chan []byte {
	out := make(chan []byte, buffer)
	s.AddOut(out)

	s.Run(func() {
		defer s.Finish()

		span := 0
		for i := 0; i < len(s.bytes); i += span {
			span = min(size, len(s.bytes[i:]))
			output := s.bytes[i : i+span]
			s.Send(output)
		}
	})

	return out
}

type ByteKernelFlow struct {
	AttachableFlow

	value byte
	input Flow
	in    <-chan []byte
}

func ByteKernel(value byte, input Flow) *ByteKernelFlow {
	bk := &ByteKernelFlow{
		value: value,
		input: input,
		in:    input.Attach(8, 256),
	}

	return bk
}

func (bk *ByteKernelFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	bk.AddOut(out)

	bk.Run(func() {
		defer bk.Finish()

		for input := range bk.in {
			count := len(input)
			needed := count / 8
			if count%8 != 0 {
				needed += 1
			}

			outputs := make([]byte, needed, needed)
			for i := 0; i < count; i++ {
				if input[i] == bk.value {
					outputs[i/8] |= 1 << uint(i%8)
				}
			}

			for _, onebyte := range outputs {
				output := make([]byte, 1, 1)
				output[0] = onebyte

				bk.Send(output)
			}
		}
	})

	return out
}

type BitByteFlow struct {
	AttachableFlow

	input Flow
	in    <-chan []byte
}

func BitByte(input Flow) *BitByteFlow {
	bb := &BitByteFlow{
		input: input,
		in:    input.Attach(8, 256),
	}

	return bb
}

func (bb *BitByteFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	bb.AddOut(out)

	bb.Run(func() {
		defer bb.Finish()

		for input := range bb.in {
			count := len(input)
			for n := 0; n < count; n++ {
				for i := 0; i < 8; i++ {
					output := make([]byte, 1, 1)
					if input[n]&(1<<uint(i)) > 0 {
						output[0] = '1'
					} else {
						output[0] = '.'
					}

					bb.Send(output)
				}
			}
		}
	})

	return out
}

type AdvanceMarkerFlow struct {
	AttachableFlow

	input Flow
	in    <-chan []byte
}

func AdvanceMarker(input Flow) *AdvanceMarkerFlow {
	am := &AdvanceMarkerFlow{
		input: input,
		in:    input.Attach(8, 256),
	}

	return am
}

func (am *AdvanceMarkerFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	am.AddOut(out)

	am.Run(func() {
		defer am.Finish()

		var carry byte
		for input := range am.in {
			count := len(input)
			for n := 0; n < count; n++ {
				fill := carry
				carry = (input[n] & high_bit) >> 7
				output := make([]byte, 1, 1)
				output[0] = (input[n] << 1) | fill

				am.Send(output)
			}
		}

		if carry != 0 {
			output := make([]byte, 1, 1)
			output[0] = carry

			am.Send(output)
		}
	})

	return out
}

type OrMarkerFlow struct {
	AttachableFlow

	inputs []Flow
	ins    []<-chan []byte
}

func OrMarker(inputs ...Flow) *OrMarkerFlow {
	om := &OrMarkerFlow{
		inputs: inputs,
	}

	for _, input := range inputs {
		om.ins = append(om.ins, input.Attach(8, 256))
	}

	return om
}

func (om *OrMarkerFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	om.AddOut(out)

	om.Run(func() {
		defer om.Finish()

		for {
			var inputs [][]byte

			for _, in := range om.ins {
				input, ok := <-in
				if !ok {
					return
				}

				inputs = append(inputs, input)
			}

			count := len(inputs[0])
			for i := 0; i < count; i++ {
				output := make([]byte, 1, 1)

				for _, input := range inputs {
					output[0] |= input[i]
				}

				om.Send(output)
			}
		}
	})

	return out
}

type AndMarkerFlow struct {
	AttachableFlow

	inputs []Flow
	ins    []<-chan []byte
}

func AndMarker(inputs ...Flow) *AndMarkerFlow {
	am := &AndMarkerFlow{
		inputs: inputs,
	}

	for _, input := range inputs {
		am.ins = append(am.ins, input.Attach(8, 256))
	}

	return am
}

func (am *AndMarkerFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	am.AddOut(out)

	am.Run(func() {
		defer am.Finish()

		for {
			var inputs [][]byte

			for _, in := range am.ins {
				input, ok := <-in
				if !ok {
					return
				}

				inputs = append(inputs, input)
			}

			count := len(inputs[0])
			for i := 0; i < count; i++ {
				output := make([]byte, 1, 1)

				for _, input := range inputs {
					output[0] &= input[i]
				}

				am.Send(output)
			}
		}
	})

	return out
}

type XorMarkerFlow struct {
	AttachableFlow

	inputs []Flow
	ins    []<-chan []byte
}

func XorMarker(inputs ...Flow) *XorMarkerFlow {
	xm := &XorMarkerFlow{
		inputs: inputs,
	}

	for _, input := range inputs {
		xm.ins = append(xm.ins, input.Attach(8, 256))
	}

	return xm
}

func (xm *XorMarkerFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	xm.AddOut(out)

	xm.Run(func() {
		defer xm.Finish()

		for {
			var inputs [][]byte

			for _, in := range xm.ins {
				input, ok := <-in
				if !ok {
					return
				}

				inputs = append(inputs, input)
			}

			count := len(inputs[0])
			for i := 0; i < count; i++ {
				output := make([]byte, 1, 1)

				for _, input := range inputs {
					output[0] ^= input[i]
				}

				xm.Send(output)
			}
		}
	})

	return out
}

type SumMarkerFlow struct {
	AttachableFlow

	inputs []Flow
	ins    []<-chan []byte
}

func SumMarker(inputs ...Flow) *SumMarkerFlow {
	sm := &SumMarkerFlow{
		inputs: inputs,
	}

	for _, input := range inputs {
		sm.ins = append(sm.ins, input.Attach(8, 256))
	}

	return sm
}

func (sm *SumMarkerFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	sm.AddOut(out)

	sm.Run(func() {
		defer sm.Finish()

		var carry byte
		for {
			var inputs [][]byte

			for _, in := range sm.ins {
				input, ok := <-in
				if !ok {
					return
				}

				inputs = append(inputs, input)
			}

			count := len(inputs[0])
			for i := 0; i < count; i++ {
				output := make([]byte, 1, 1)

				for _, input := range inputs {
					sum := uint16(carry) + uint16(output[0]) + uint16(input[i])
					if sum >= 256 {
						carry = byte(sum >> 7)
						sum <<= 7
						sum >>= 7
					}
					output[0] = byte(sum)
				}

				sm.Send(output)
			}
		}
	})

	return out
}

type MatchStarMarkerFlow struct {
	AttachableFlow

	prev Flow
	star Flow
}

func MatchStarMarker(prev Flow, star Flow) *MatchStarMarkerFlow {
	msm := &MatchStarMarkerFlow{
		prev: prev,
		star: star,
	}

	return msm
}

func (msm *MatchStarMarkerFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	msm.AddOut(out)

	msm.Run(func() {
		defer msm.Finish()

		t0 := AndMarker(msm.prev, msm.star)
		t1 := SumMarker(t0, msm.star)
		t2 := XorMarker(t1, msm.star)
		m2 := OrMarker(t2, msm.star)

		for output := range m2.Attach(8, 256) {
			msm.Send(output)
		}
	})

	return out
}

type ChunkFlow struct {
	AttachableFlow
	ReducingFlow

	size   int
	inputs []Flow
}

func Chunk(size int, inputs []Flow) *ChunkFlow {
	c := &ChunkFlow{
		size:   size,
		inputs: inputs,
	}

	for _, input := range inputs {
		c.AddIn(input.Attach(8, 256))
	}

	return c
}

func (c *ChunkFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	c.AddOut(out)

	c.Run(func() {
		defer c.Finish()

		// We will accumulate data from each of the input streams until
		// we have enough data to output a complete chunk.
		var acc [][]byte

		// We will need to track the readiness of the accumulator to be
		// emptied. It is ready if we've accumulated enough data from
		// each input stream to output one complete chunk.
		var ready []bool

		// Flush any remaining accumulated data.
		var flushed []bool

		for range c.ins {
			acc = append(acc, make([]byte, 0, c.size))
			ready = append(ready, false)
			flushed = append(flushed, false)
		}

		for all_done := false; !all_done; {
			// Read in one chunk of input from each input stream
			// and add to our accumulators.
			c.Receive(func(inputs ...[]byte) {
				count := 0
				for x, input := range inputs {
					if input != nil {
						count += 1

						acc[x] = append(acc[x], input...)
						if len(acc[x]) >= c.size {
							ready[x] = true
						}
					}
				}

				if count == 0 {
					all_done = true
				}
			})

			// Have we accumulated enough to output a chunk?
			if all(ready) {
				var chunk []byte

				for x := range acc {
					chunk = append(chunk, acc[x][:c.size]...)
					chunk = append(chunk, '\n')
					acc[x] = acc[x][c.size:]

					if len(acc[x]) >= c.size {
						ready[x] = true
					} else {
						ready[x] = false
					}
				}

				chunk = append(chunk, '\n')
				c.Send(chunk)
			}

		}

		for !all(flushed) {
			var chunk []byte

			for x := range acc {
				if !flushed[x] {
					// As big as our fixed size or at least
					// all the remaining bytes.
					span := min(c.size, len(acc[x]))

					chunk = append(chunk, acc[x][:span]...)
					chunk = append(chunk, '\n')
					acc[x] = acc[x][span:]

					if len(acc[x]) == 0 {
						flushed[x] = true
					}
				}
			}

			chunk = append(chunk, '\n')
			c.Send(chunk)
		}
	})

	return out
}

type WriterFlow struct {
	AttachableFlow

	writer io.Writer
	input  Flow
	in     <-chan []byte
}

func ToWriter(writer io.Writer, input Flow) *WriterFlow {
	w := &WriterFlow{
		writer: writer,
		input:  input,
		in:     input.Attach(8, 256),
	}

	return w
}

func (w *WriterFlow) Attach(buffer, _ int) <-chan []byte {
	out := make(chan []byte, buffer)
	w.AddOut(out)

	w.Run(func() {
		defer w.Finish()

		for input := range w.in {
			w.writer.Write(input)
		}
	})

	return out
}

func main() {
	fmt.Println("Starting up...")

	// Our test data...
	//input := FromString("Axe; Apples; A badApple; Accede; Ate!")
	input := FromString("A bunch of bytes with spaces.  bbb  ")

	// Implementing the regex: [ ]b*[ ]
	spaces := ByteKernel(' ', input)
	char_b := ByteKernel('b', input)

	am_spc := AdvanceMarker(spaces)
	msm_cb := MatchStarMarker(am_spc, char_b)

	am_chb := AdvanceMarker(char_b)
	and_mr := AndMarker(msm_cb, am_chb)

	//spaces_bits := BitByte(spaces)
	//char_b_bits := BitByte(char_b)

	//am_spc_bits := BitByte(am_spc)
	am_chb_bits := BitByte(am_chb)

	msm_cb_bits := BitByte(msm_cb)
	and_mr_bits := BitByte(and_mr)

	flows := []Flow{
		input,
		//spaces_bits,
		//char_b_bits,
		//am_spc_bits,
		am_chb_bits,
		msm_cb_bits,
		and_mr_bits,
	}
	aligned := Chunk(16, flows)

	output := ToWriter(os.Stdout, aligned)
	DrainFlow(output)
}
