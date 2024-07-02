const composer = require("openwhisk-composer");

xkte = composer.action("/guest/debug/node_0");
ldkz = composer.action("/guest/debug/node_1");
qepf = composer.action("/guest/debug/node_2");
jdre = composer.action("/guest/debug/node_3");
bpih = composer.action("/guest/debug/node_4");

// Iteration1: Parallel
evsw = composer.parallel(ldkz, qepf, jdre);

// Iteration2: Sequence
evhw = composer.sequence(xkte, evsw, bpih);

module.exports = evhw;
