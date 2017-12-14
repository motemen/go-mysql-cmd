package mysqlcmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os/exec"
)

var delimiter = "__mysql_cmd__"

type mediator struct {
	cmd    *exec.Cmd
	r      io.ReadCloser
	w      io.WriteCloser
	errBuf *bytes.Buffer
}

func (m *mediator) Exec(command string) ([][][]byte, error) {
	log.Printf("debug: Exec: %q", command)

	_, err := fmt.Fprintf(m.w, "%s; SELECT '%s';\n", command, delimiter)
	if err != nil {
		return nil, err
	}

	rows := [][][]byte{}

	s := bufio.NewScanner(m.r)
	for s.Scan() {
		if s.Text() == delimiter {
			return rows, nil
		} else {
			fields := bytes.Split(s.Bytes(), []byte("\t"))
			rows = append(rows, fields)
		}
	}

	return rows, s.Err()
}

func (m *mediator) Close() error {
	m.w.Close()
	m.r.Close()
	return m.cmd.Wait()
}

func (m *mediator) Err() error {
	errString := m.errBuf.String()
	if errString != "" {
		return fmt.Errorf("%s", errString)
	}
	return nil
}

func New(args ...string) (*mediator, error) {
	args = append(args, "--skip-column-names", "--batch", "--unbuffered")

	var errBuf bytes.Buffer
	cmd := exec.Command("mysql", args...)
	cmd.Stderr = &errBuf

	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	w, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	m := &mediator{
		cmd:    cmd,
		r:      r,
		w:      w,
		errBuf: &errBuf,
	}

	_, err = m.Exec("SET SESSION wait_timeout = 3600")
	return m, err
}
