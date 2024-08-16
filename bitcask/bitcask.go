package bitcask

type Bitcask struct {
	F   *FileMgr
	Idx *Index
	Opt *Option
	Dir string
}

func Open(dir string) *Bitcask {

}
