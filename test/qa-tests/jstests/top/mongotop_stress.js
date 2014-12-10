// mongotop_stress.js; ensure that running mongotop - even when the server is
// under heavy load - works as expected
//
var testName = 'mongotop_stress';
load('jstests/common/topology_helper.js');

(function() {
  jsTest.log('Testing mongotop\'s performance under load');

  var extractJSON = function(shellOutput) {
    return shellOutput.substring(shellOutput.indexOf('{'), shellOutput.lastIndexOf('}') + 1);
  };

  var runTests = function(topology, passthrough) {
    jsTest.log('Using ' + passthrough.name + ' passthrough');
    var t = topology.init(passthrough);
    var conn = t.connection();
    db = conn.getDB('foo');
    var pt = passthrough;
    var at = auth;

    var writeShell = '\nprint(\'starting write\'); \n' +
    'db.getSiblingDB(\'admin\').auth(\'' + authUser + '\',\'' + authPassword + '\'); \n' +
    'var dbName = (Math.random() + 1).toString(36).substring(7); \n' +
    'var clName = (Math.random() + 1).toString(36).substring(7); \n' +
    'for (var i = 0; i < 1000000; ++i) \n{ ' +
    '  db.getSiblingDB(dbName).getCollection(clName).insert({ x: i }); \n' +
    '  sleep(1); \n' +
    '}\n';
 
    // start the parallel shell commands
    for (var i = 0; i < 10; i++) {
      startParallelShell(writeShell);
    }

    // ensure tool runs without error
    clearRawMongoProgramOutput();
    assert.eq(runMongoProgram.apply(this, ['mongotop', '--port', conn.port, '--json', '--rowcount', 1].concat(passthrough.args)), 0, 'failed 1');

    t.stop();
  };

  // run with plain and auth passthroughs
  passthroughs.forEach(function(passthrough) {
    runTests(standaloneTopology, passthrough);
    runTests(replicaSetTopology, passthrough);
  });
})();