package commands

import (
	"context"
	"fmt"
	"io"
	"os"

	cmds "github.com/ipfs/go-ipfs-cmds"
	"github.com/ipfs/go-ipfs-cmds/cmdsutil"
	oldCmds "github.com/ipfs/go-ipfs/commands"

	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/filestore"
	cid "gx/ipfs/QmV5gPoRsjN1Gid3LMdNZTyfCtP2DsvqEbMAmz82RmmiGk/go-cid"
	//u "gx/ipfs/QmZuY8aV7zbNXVy6DyN9SmnuH3o9nG852F4aTiSBpts8d1/go-ipfs-util"
)

var FileStoreCmd = &cmds.Command{
	Helptext: cmdsutil.HelpText{
		Tagline: "Interact with filestore objects.",
	},
	Subcommands: map[string]*cmds.Command{
		"ls": lsFileStore,
	},
	OldSubcommands: map[string]*oldCmds.Command{
		"verify": verifyFileStore,
		"dups":   dupsFileStore,
	},
}

type lsEncoder struct {
	errors bool
	w      io.Writer
}

var lsFileStore = &cmds.Command{
	Helptext: cmdsutil.HelpText{
		Tagline: "List objects in filestore.",
		LongDescription: `
List objects in the filestore.

If one or more <obj> is specified only list those specific objects,
otherwise list all objects.

The output is:

<hash> <size> <path> <offset>
`,
	},
	Arguments: []cmdsutil.Argument{
		cmdsutil.StringArg("obj", false, true, "Cid of objects to list."),
	},
	Run: func(req cmds.Request, re cmds.ResponseEmitter) {
		_, fs, err := getFilestore(req.InvocContext())
		if err != nil {
			re.SetError(err, cmdsutil.ErrNormal)
			return
		}
		args := req.Arguments()
		if len(args) > 0 {
			out := perKeyActionToChan(args, func(c *cid.Cid) *filestore.ListRes {
				return filestore.List(fs, c)
			}, req.Context())

			for v := range out {
				re.Emit(v)
			}
		} else {
			next, err := filestore.ListAll(fs)
			if err != nil {
				re.SetError(err, cmdsutil.ErrNormal)
				return
			}

			out := listResToChan(next, req.Context())
			for v := range out {
				log.Debugf("%T", v)
				re.Emit(v)
			}
		}
	},
	PostRun: map[cmds.EncodingType]func(cmds.Request, cmds.ResponseEmitter) cmds.ResponseEmitter{
		cmds.CLI: func(req cmds.Request, re cmds.ResponseEmitter) cmds.ResponseEmitter {
			re_, res := cmds.NewChanResponsePair(req)

			go func() {
				defer re.Close()

				var (
					err    error
					errors bool
				)

				for err == nil {
					var v interface{}

					v, err = res.Next()
					if err != nil {
						break
					}

					r := v.(*filestore.ListRes)
					if r.ErrorMsg != "" {
						errors = true
						fmt.Fprintf(os.Stderr, "%s\n", r.ErrorMsg)
					} else {
						fmt.Fprintf(os.Stdout, "%s\n", r.FormatLong())
					}
				}

				if err == io.EOF || err.Error() == "EOF" {
					// all good
				} else if err == cmds.ErrRcvdError {
					e := res.Error()
					re.SetError(e.Message, e.Code)
				} else {
					re.SetError(err, cmdsutil.ErrNormal)
				}

				if errors {
					re.SetError("errors while displaying some entries", cmdsutil.ErrNormal)
				}
			}()

			return re_
		},
	},
	Type: filestore.ListRes{},
}

var verifyFileStore = &oldCmds.Command{
	Helptext: cmdsutil.HelpText{
		Tagline: "Verify objects in filestore.",
		LongDescription: `
Verify objects in the filestore.

If one or more <obj> is specified only verify those specific objects,
otherwise verify all objects.

The output is:

<status> <hash> <size> <path> <offset>

Where <status> is one of:
ok:       the block can be reconstructed
changed:  the contents of the backing file have changed
no-file:  the backing file could not be found
error:    there was some other problem reading the file
missing:  <obj> could not be found in the filestore
ERROR:    internal error, most likely due to a corrupt database

For ERROR entries the error will also be printed to stderr.
`,
	},
	Arguments: []cmdsutil.Argument{
		cmdsutil.StringArg("obj", false, true, "Cid of objects to verify."),
	},
	Run: func(req oldCmds.Request, res oldCmds.Response) {
		_, fs, err := getFilestore(req.InvocContext())
		if err != nil {
			res.SetError(err, cmdsutil.ErrNormal)
			return
		}
		args := req.Arguments()
		if len(args) > 0 {
			out := perKeyActionToChan(args, func(c *cid.Cid) *filestore.ListRes {
				return filestore.Verify(fs, c)
			}, req.Context())
			res.SetOutput(out)
		} else {
			next, err := filestore.VerifyAll(fs)
			if err != nil {
				res.SetError(err, cmdsutil.ErrNormal)
				return
			}
			out := listResToChan(next, req.Context())
			res.SetOutput(out)
		}
	},
	Marshalers: oldCmds.MarshalerMap{
		oldCmds.Text: func(res oldCmds.Response) (io.Reader, error) {
			v := unwrapOutput(res.Output())
			r := v.(*filestore.ListRes)
			if r.Status == filestore.StatusOtherError {
				fmt.Fprintf(res.Stderr(), "%s\n", r.ErrorMsg)
			}
			fmt.Fprintf(res.Stdout(), "%s %s\n", r.Status.Format(), r.FormatLong())
			return nil, nil
		},
	},
	Type: filestore.ListRes{},
}

var dupsFileStore = &oldCmds.Command{
	Helptext: cmdsutil.HelpText{
		Tagline: "List blocks that are both in the filestore and standard block storage.",
	},
	Run: func(req oldCmds.Request, res oldCmds.Response) {
		_, fs, err := getFilestore(req.InvocContext())
		if err != nil {
			res.SetError(err, cmdsutil.ErrNormal)
			return
		}
		ch, err := fs.FileManager().AllKeysChan(req.Context())
		if err != nil {
			res.SetError(err, cmdsutil.ErrNormal)
			return
		}

		out := make(chan interface{}, 128)
		res.SetOutput((<-chan interface{})(out))

		go func() {
			defer close(out)
			for cid := range ch {
				have, err := fs.MainBlockstore().Has(cid)
				if err != nil {
					out <- &RefWrapper{Err: err.Error()}
					return
				}
				if have {
					out <- &RefWrapper{Ref: cid.String()}
				}
			}
		}()
	},
	Marshalers: refsMarshallerMap,
	Type:       RefWrapper{},
}

type getNoder interface {
	GetNode() (*core.IpfsNode, error)
}

func getFilestore(g getNoder) (*core.IpfsNode, *filestore.Filestore, error) {
	n, err := g.GetNode()
	if err != nil {
		return nil, nil, err
	}
	fs := n.Filestore
	if fs == nil {
		return n, nil, fmt.Errorf("filestore not enabled")
	}
	return n, fs, err
}

func listResToChan(next func() *filestore.ListRes, ctx context.Context) <-chan interface{} {
	out := make(chan interface{}, 128)
	go func() {
		defer close(out)
		for {
			r := next()
			if r == nil {
				return
			}
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func perKeyActionToChan(args []string, action func(*cid.Cid) *filestore.ListRes, ctx context.Context) <-chan interface{} {
	out := make(chan interface{}, 128)
	go func() {
		defer close(out)
		for _, arg := range args {
			c, err := cid.Decode(arg)
			if err != nil {
				out <- &filestore.ListRes{
					Status:   filestore.StatusOtherError,
					ErrorMsg: fmt.Sprintf("%s: %v", arg, err),
				}
				continue
			}
			r := action(c)
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
