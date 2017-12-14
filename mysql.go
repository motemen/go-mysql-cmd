package mysqlcmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

var Delimiter = "!!!__mysql_cmd__!!!"

type Cmd struct {
	Delimiter string
	cmd       *exec.Cmd
	r         io.ReadCloser
	w         io.WriteCloser
	errBuf    *bytes.Buffer
}

func (m *Cmd) Exec(command string) ([][]string, error) {
	delimiter := m.Delimiter
	if delimiter == "" {
		delimiter = Delimiter
	}
	_, err := fmt.Fprintf(m.w, "%s; SELECT '%s';\n", command, delimiter)
	if err != nil {
		return nil, err
	}

	rows := [][]string{}

	s := bufio.NewScanner(m.r)
	for s.Scan() {
		if s.Text() == delimiter {
			return rows, nil
		} else {
			fields := strings.Split(s.Text(), "\t")
			rows = append(rows, fields)
		}
	}

	return rows, s.Err()
}

func (m *Cmd) Close() error {
	m.w.Close()
	m.r.Close()
	return m.cmd.Wait()
}

func (m *Cmd) Err() error {
	errString := m.errBuf.String()
	if errString != "" {
		return fmt.Errorf("%s", errString)
	}
	return nil
}

func New(args ...string) (*Cmd, error) {
	args = append(args, "--skip-column-names", "--batch", "--unbuffered")

	var errBuf bytes.Buffer
	cmd := exec.Command("mysql", args...)
	// cmd.Stderr = &errBuf
	cmd.Stderr = os.Stderr

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

	m := &Cmd{
		cmd:    cmd,
		r:      r,
		w:      w,
		errBuf: &errBuf,
	}

	_, err = m.Exec("SET SESSION wait_timeout = 3600")
	return m, err
}

var unescaper = strings.NewReplacer(
	`\n`, "\n",
	`\t`, "\t",
	`\0`, "\x00",
	`\\`, "\\",
)

func UnescapeString(s string) string {
	return unescaper.Replace(s)
}
