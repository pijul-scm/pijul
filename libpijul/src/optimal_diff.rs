/*
Copyright Florent Becker and Pierre-Etienne Meunier 2015.

This file is part of Pijul.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

pub mod diff {

    use super::super::patch;
    use super::super::graph;
    use super::super::patch::{external_key,Edge, KEY_SIZE, Change};

    use super::super::backend::*;
    use super::super::graph::{Graph, PSEUDO_EDGE, FOLDER_EDGE, PARENT_EDGE, DELETED_EDGE};
    use std;
    use std::path::Path;
    use std::io::Read;

    fn delete_edges<T>(ws:&mut Workspace, repository:&Transaction<T>, branch:&Db, edges:&mut Vec<Edge>, key:&[u8],flag:u8) {
        if key.len() > 0 {
            let ext = repository.db_external();
            let ext_key=external_key(&ext,key);
            for (k,v) in branch.iter(ws, key, Some(&[flag])) {
                if k==key && v[0] >= flag && v[0] <= flag | (PSEUDO_EDGE|FOLDER_EDGE) {
                    if v[0]&PSEUDO_EDGE == 0 {
                        edges.push(Edge {
                            from:ext_key.clone(),
                            to:external_key(&ext, &v[1..(1+KEY_SIZE)]),
                            introduced_by:external_key(&ext, &v[(1+KEY_SIZE)..]) });
                    }
                } else {
                    break
                }
            }
        }
    }

    fn add_lines<T>(repository:&Transaction<T>, line_num:&mut usize, up_context:&[u8],
                    down_context:&[&[u8]], lines:&[&[u8]])
                    -> patch::Change
    {
        let ext = repository.db_external();
        let changes = Change::NewNodes {
            up_context:vec!(external_key(&ext, up_context)),
            down_context:down_context.iter().map(|x|{external_key(&ext, x)}).collect(),
            line_num: *line_num as u32,
            flag:0,
            nodes:lines.iter().map(|x|{x.to_vec()}).collect()
        };
        *line_num += lines.len();
        changes
    }


    fn delete_lines<T>(ws:&mut Workspace, repository:&Transaction<T>, branch:&Db, lines:&[&[u8]]) -> Change
    {
        let mut edges=Vec::with_capacity(lines.len());
        for i in 0..lines.len() {
            //debug!(target:"conflictdiff","deleting line {}",lines[i].to_hex());
            delete_edges(ws, repository, branch, &mut edges, lines[i], PARENT_EDGE)
        }
        Change::Edges{edges:edges, flag:PARENT_EDGE|DELETED_EDGE}
    }

    fn local_diff<T>(ws:&mut Workspace, repository:&Transaction<T>, branch:&Db, actions:&mut Vec<Change>,
                     line_num:&mut usize, lines_a:&[&[u8]], contents_a:&[Contents], b:&[&[u8]])
    {
        debug!(target:"conflictdiff","local_diff {} {}",contents_a.len(),b.len());
        let mut opt=vec![vec![0;b.len()+1];contents_a.len()+1];
        if contents_a.len()>0 {
            let mut i=contents_a.len() - 1;
            loop {
                opt[i]=vec![0;b.len()+1];
                if b.len()>0 {
                    let mut j=b.len()-1;
                    loop {
                        let mut contents_a_i = contents_a[i].clone();
                        opt[i][j]=
                            if super::super::eq(&mut contents_a_i, &mut Contents::from_slice(&b[j])) {
                                opt[i+1][j+1]+1
                            } else {
                                std::cmp::max(opt[i+1][j], opt[i][j+1])
                            };
                        debug!(target:"diff","opt[{}][{}] = {}",i,j,opt[i][j]);
                        if j>0 { j-=1 } else { break }
                    }
                }
                if i>0 { i-=1 } else { break }
            }
        }
        let mut i=1;
        let mut j=0;
        let mut oi=None;
        let mut oj=None;
        let mut last_alive_context=0;
        while i<contents_a.len() && j<b.len() {
            debug!(target:"diff","i={}, j={}",i,j);
            let mut contents_a_i = contents_a[i].clone();
            if super::super::eq(&mut contents_a_i, &mut Contents::from_slice(&b[j])) {
                if let Some(i0)=oi {
                    debug!(target:"diff","deleting from {} to {} / {}",i0,i,lines_a.len());
                    let dels = delete_lines(ws, repository, branch, &lines_a[i0..i]);
                    actions.push(dels);
                    oi=None
                } else if let Some(j0)=oj {
                    let adds = add_lines(repository, line_num,
                                         lines_a[last_alive_context], // up context
                                         &lines_a[i..i+1], // down context
                                         &b[j0..j]);
                    actions.push(adds);
                    oj=None
                }
                last_alive_context=i;
                i+=1; j+=1;
            } else {
                if opt[i+1][j] >= opt[i][j+1] {
                    // we will delete things starting from i (included).
                    if let Some(j0)=oj {
                        let adds = add_lines(repository,
                                             line_num,
                                             lines_a[last_alive_context], // up context
                                             &lines_a[i..i+1], // down context
                                             &b[j0..j]);
                        actions.push(adds);
                        oj=None
                    }
                    if oi.is_none() {
                        oi=Some(i)
                    }
                    i+=1
                } else {
                    // We will add things starting from j.
                    if let Some(i0)=oi {
                        let dels = delete_lines(ws, repository, branch, &lines_a[i0..i]);
                        actions.push(dels);
                        last_alive_context=i0-1;
                        oi=None
                    }
                    if oj.is_none() {
                        oj=Some(j)
                    }
                    j+=1
                }
            }
        }
        if i < lines_a.len() {
            if let Some(j0)=oj {
                let adds = add_lines(repository, line_num,
                                     lines_a[i-1], // up context
                                     &lines_a[i..i+1], // down context
                                     &b[j0..j]);
                actions.push(adds)
                    
            }
            let dels = delete_lines(ws, repository, branch, &lines_a[i..lines_a.len()]);
            actions.push(dels)
        } else if j < b.len() {
            if let Some(i0)=oi {
                delete_lines(ws, repository, branch, &lines_a[i0..i]);
                let adds =
                    add_lines(repository, line_num, lines_a[i0-1], &[], &b[j..b.len()]);
                actions.push(adds);
            } else {
                let adds = add_lines(repository, line_num, lines_a[i-1], &[], &b[j..b.len()]);
                actions.push(adds);
            }
        }
    }
    

    struct Diff<'a> {
        lines_a:Vec<&'a[u8]>,
        contents_a:Vec<Contents<'a>>
    }

    impl <'a> graph::LineBuffer<'a> for Diff<'a> {
        fn output_line(&mut self,k:&'a[u8],c:Contents<'a>) {
            //println!("outputting {:?} {}",k,unsafe {std::str::from_utf8_unchecked(c)});
            self.lines_a.push(k);
            self.contents_a.push(c);
        }
    }

    pub fn diff<'a,T>(repository:&Transaction<T>,branch:&Db,line_num:&mut usize, actions:&mut Vec<Change>,
                    redundant:&mut Vec<u8>,
                    a:Graph<'a>, b:&Path)->Result<(),std::io::Error> {
        
        let mut buf_b=Vec::new();
        let mut lines_b=Vec::new();
        let err={
            let err={
                let f = std::fs::File::open(b);
                let mut f = std::io::BufReader::new(f.unwrap());
                f.read_to_end(&mut buf_b)
            };
            let mut i=0;
            let mut j=0;

            while j<buf_b.len() {
                if buf_b[j]==0xa {
                    lines_b.push(&buf_b[i..j+1]);
                    i=j+1
                }
                j+=1;
            }
            if i<j { lines_b.push(&buf_b[i..j]) }
            err
        };
        match err {
            Ok(_)=>{
                //let t0=time::precise_time_s();
                let db_contents = repository.db_contents();
                let mut d = Diff { lines_a:Vec::new(), contents_a:Vec::new() };
                let mut ws = Workspace::new();
                graph::output_file(&mut ws, branch, &db_contents, &mut d,a,redundant);
                //let t1=time::precise_time_s();
                //info!("output_file took {}s",t1-t0);
                local_diff(&mut ws, repository, branch, actions, line_num,
                           &d.lines_a,
                           &d.contents_a[..],
                           &lines_b);
                //let t2=time::precise_time_s();
                //info!("diff took {}s",t2-t1);
                Ok(())
            },
            Err(e)=>Err(e)
        }
    }
}
