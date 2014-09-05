# batchdb

queue batch jobs and stream results to a blob store

# example

## adding a job

First, we can add a job. Adding a job both creates the job and pushes it onto
the pending queue:

``` js
var batchdb = require('batchdb');
var db = require('level')('/tmp/compute.db');

var compute = batchdb(db, { path: '/tmp/compute.blobs' });
compute.add().end('sleep 5; date');

compute.on('create', function (key) {
    console.log('created', key);
});

compute.on('push', function (key, created) {
    console.log('pushed', key, created);
});
```

Now we'll schedule a few jobs:

```
$ node add.js
created ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0
pushed ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0 1409895158130
$ node add.js
created ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0
pushed ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0 1409895160930
```

The job ID is the same in this case because the job content is identical, but
the job was still scheduled to run twice.

## listing pending jobs

Now that jobs have been added, they show up in the pending list:

``` js
var batchdb = require('batchdb');
var db = require('level')('/tmp/compute.db');
var compute = batchdb(db, { path: '/tmp/compute.blobs' });

compute.list('pending').on('data', console.log);
```

```
{ job: 'ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0',
  created: 1409895158130,
  running: false }
{ job: 'ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0',
  created: 1409895160930,
  running: false }
```

## running jobs

Now we can tell the jobs to run by calling `compute.run()` and providing a run
function that will store the batch results:

``` js
var batchdb = require('batchdb');
var spawn = require('child_process').spawn;
var duplexer = require('duplexer');

var db = require('level')('/tmp/compute.db');
var compute = batchdb(db, { path: '/tmp/compute.blobs', run: run });

function run (key) {
    var ps = spawn('bash');
    return duplexer(ps.stdin, ps.stdout);
}

compute.on('result', function (key, id) {
    console.log('RESULT', key, id);
    compute.getResult(id).pipe(process.stdout);
});

compute.run();
```

```
$ node run.js 
RESULT ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0 0469e2796db49df219ca5d9380dddd5b48f9a7fe930945d1988aa966a62f1e0a
Thu Sep  4 22:36:12 PDT 2014
RESULT ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0 4c2d44f035f6fa04a1246806fda7f881347b96b8ff821a0a5aacad08e904d06c
Thu Sep  4 22:36:17 PDT 2014
```

# methods

``` js
var batchdb = require('batchdb')
```

## var compute = batchdb(db, opts)

## var ws = compute.create(cb)

Create a new job from the payload written to the writable stream `ws`.

After payload has been saved and the job has been written to the database,
`cb(err, jobkey)` fires with the `jobkey` id.

## compute.push(jobkey, cb)

Push a job to the end of the pending queue by its key, `jobkey`.

## var ws = compute.add(cb)

Create a job and push it to the pending queue in one step. This is similar to
calling `.create()` followed by `.push()`, but more atomic.

## compute.run()

Process the pending queue by most-recent first.

## var rs = compute.list(type)

Return a readable object stream `rs` that contains rows for each `type`:

* `'job'`
* `'pending'`
* `'result'`

example `job` object:

```
{ key: 'ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0',
  running: [] }
```

example `pending` object:

```
{ job: 'ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0',
  created: 1409896340875,
  running: false }
```

example `result` object:

```
{ key: 
   [ 'ce6cc4e7fcb4a902f7c836cb93a32ae02cb47679c86e3d99c811233bf6f258c0',
     1409895158130 ],
  value: 
   { hash: '0469e2796db49df219ca5d9380dddd5b48f9a7fe930945d1988aa966a62f1e0a',
     start: 1409895367770,
     end: 1409895372831,
     created: 1409895158130 } }
```

# install

With [npm](https://npmjs.org) do:

```
npm install batchdb
```

## license

MIT
