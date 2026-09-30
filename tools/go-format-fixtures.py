#!/usr/bin/env python3
"""Maak formaatfixtures met de bestaande Go-implementatie; verwijder de testhulp altijd."""
import os
from pathlib import Path
import subprocess
ROOT=Path(__file__).resolve().parents[1]
SPIN=Path(os.environ.get('SPIN_DIR',ROOT.parent/'spin'))
OUT=ROOT/'core/tests/fixtures'
SOURCE=r'''package replica
import("bytes";"context";"encoding/binary";"encoding/json";"os";"path/filepath";"testing";"time")
func TestRustPortFormats(t *testing.T){
  dir:=os.Getenv("REPLICA_FIXTURES")
  save:=func(name string,b []byte){if e:=os.WriteFile(filepath.Join(dir,name),b,0600);e!=nil{t.Fatal(e)}}
  seg:=segment{PageSize:512,DBSize:1536,Pages:[]uint32{3,1,2},Data:[][]byte{bytes.Repeat([]byte{0x33},512),bytes.Repeat([]byte{0x11},512),bytes.Repeat([]byte{0x22},512)}}
  data:=encodeSegment(seg); save("segment.bin",data)
  generation:="20260930-123456-abcdef0123456789"
  header:=make([]byte,dirtyLogHeaderSize(generation));copy(header,dirtyLogMagic)
  binary.BigEndian.PutUint64(header[8:],7);binary.BigEndian.PutUint64(header[16:],^uint64(7));binary.BigEndian.PutUint16(header[24:],uint16(len(generation)));copy(header[26:],generation);copy(header[len(header)-8:],dirtyLogChecksum(header))
  for _,n:=range []uint32{3,1,3,2}{var b [8]byte;binary.BigEndian.PutUint32(b[:],n);binary.BigEndian.PutUint32(b[4:],^n);header=append(header,b[:]...)}
  save("dirty.bin",header)
  at:=time.Date(2026,9,30,12,34,56,123456789,time.UTC)
  m:=manifest{Version:2,MinSize:0,FirstSeq:1,Seq:1,At:at,Parts:[]partRef{{Key:"replica/test/gen/data/one",Size:int64(len(data)),Hash:sha256hex(data)}}}
  b,e:=json.Marshal(m);if e!=nil{t.Fatal(e)};save("manifest.json",b)
  m.FirstSeq=2;m.Seq=10;m.Level=1;m.Start=at.Add(-time.Hour);m.End=at
  b,e=json.Marshal(m);if e!=nil{t.Fatal(e)};save("window.json",b)
  mk:=marker{Destination:sha256hex([]byte("destination")),PageSize:512,Version:2,At:at,Generation:"20260930T123456Z-abcdef",Seq:7,Size:1536,Bytes:10000,Complete:true,Clean:true,StartedAt:at.Add(-time.Hour)}
  b,e=json.Marshal(mk);if e!=nil{t.Fatal(e)};save("marker.json",b)
  next:=marker{Destination:mk.Destination,Version:2,Generation:"20260930T143456Z-new",StartedAt:at.Add(time.Hour),Previous:&mk,Uncertain:1}
  b,e=json.Marshal(next);if e!=nil{t.Fatal(e)};save("renewal.json",b)
  objects:=newMemoryObjects();r:=Replica{config:Config{Prefix:"replica",SegmentBytes:65536},domain:"test",s3:objects,now:time.Now}
  generation="20260930T123456Z-abcdef"
  part2,e:=r.putPart(context.Background(),r.generationPrefix(generation)+"data/raw2",segment{PageSize:512,DBSize:512,Pages:[]uint32{1},Data:[][]byte{bytes.Repeat([]byte{9},512)}});if e!=nil{t.Fatal(e)}
  part3,e:=r.putPart(context.Background(),r.generationPrefix(generation)+"data/raw3",segment{PageSize:512,DBSize:1536,Pages:[]uint32{3},Data:[][]byte{bytes.Repeat([]byte{7},512)}});if e!=nil{t.Fatal(e)}
  inputs:=[]manifest{{Version:2,MinSize:512,FirstSeq:2,Seq:2,At:at.Add(time.Second),Parts:[]partRef{part2}},{Version:2,MinSize:1536,FirstSeq:3,Seq:3,At:at.Add(2*time.Second),Parts:[]partRef{part3}}}
  merged,e:=r.mergeWindow(context.Background(),generation,1,at,at.Add(10*time.Second),inputs);if e!=nil{t.Fatal(e)}
  if len(merged.Parts)!=1{t.Fatal("expected one compacted part")};save("compact.bin",objects.data[merged.Parts[0].Key])
}
'''
OUT.mkdir(parents=True,exist_ok=True)
helper=SPIN/'replica/rust_port_fixture_test.go'
if helper.exists(): raise RuntimeError('fixture helper already exists')
try:
    helper.write_text(SOURCE)
    subprocess.run(['go','test','./replica','-run','^TestRustPortFormats$','-count=1'],cwd=SPIN,env={**os.environ,'GOWORK':'off','REPLICA_FIXTURES':str(OUT)},check=True)
finally:
    helper.unlink()
