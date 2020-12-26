const Benchmark = require('benchmark');
const suite = new Benchmark.Suite;
const hash = require('crypto').createHash;
const fnv = require('../fnv')
const HashFNV = require('hash.fnv.crypto')
const data = 'Delightful remarkably mr on announcing themselves entreaties favourable. About to in so terms voice at. Equal an would is found seems of. The particular friendship one sufficient terminated frequently themselves. It more shed went up is roof if loud case. Delay music in lived noise an. Beyond genius really enough passed is up.';
const scenarios = [
  { alg: 'md5', digest: 'hex' },
  { alg: 'md5', digest: 'base64' },
  { alg: 'sha1', digest: 'hex' },
  { alg: 'sha1', digest: 'base64' },
  { alg: 'sha256', digest: 'hex' },
  { alg: 'sha256', digest: 'base64' }
];
{
    const h = hash('sha1')
    h.write(data)
    h.end()
    console.log(h.read().toString('hex'))
}
suite.add('fnv.crypto', () => {
    const hash = new HashFNV(0)
    hash.update(data)
    hash.digest('hex')
})
suite.add('sha1.stream', () => {
    const h = hash('sha1')
    h.write(data)
    h.end()
    h.read().toString('hex')
})
for (const { alg, digest } of scenarios) {
  suite.add(`${alg}-${digest}`, () =>
     hash(alg).update(data).digest(digest)
  );
}
suite.add('fnv', () => {
    fnv(Buffer.from(data))
})
suite.on('cycle', function(event) {
  console.log(String(event.target));
})
.on('complete', function() {
  console.log('Fastest is ' + this.filter('fastest').map('name'));
})
.run();
