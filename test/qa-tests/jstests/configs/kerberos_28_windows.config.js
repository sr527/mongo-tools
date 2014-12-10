/** NOTE: this config uses a static Kerberos instance running on an EC2
 *  machine outside our security group. It should NOT be used for
 *  automated tests, because its a single instance and there's no
 *  automated way to generate more instances just yet. */

/** NOTE: you need to add a registry entry for the MADHACKER.BIZ Kerberos
 *  realm before using this:
 *  cmd /c "REG ADD HKLM\SYSTEM\ControlSet001\Control\Lsa\Kerberos\Domains\MADHACKER.BIZ /v KdcNames /d karpov.madhacker.biz /t REG_MULTI_SZ /f"
 */

var getToolTest;

(function() {
  var AUTH_USER = 'luke.skywalker@MADHACKER.BIZ';

  getToolTest = function(name) {
    var toolTest = new ToolTest(name, {});
    var db;

    db = toolTest.db = new Mongo('karpov.madhacker.biz:27017').getDB('test');

    /** Overwrite so toolTest.runTool doesn't append --host */
    ToolTest.prototype.runTool = function() {
      arguments[0] = 'mongo' + arguments[0];
      return runMongoProgram.apply(null , arguments);
    };

    db.getSiblingDB('$external').auth({
      user: AUTH_USER,
      pwd: AUTH_PASSWORD,
      mechanism: 'GSSAPI',
      serviceName: 'mongodb',
      serviceHostname: 'karpov.madhacker.biz'
    });

    toolTest.authCommand = 'db.getSiblingDB(\'$external\').auth({ user: ' +
      '\'' + AUTH_USER + '\', pwd: \'' + AUTH_PASSWORD +'\', ' +
      'mechanism: \'GSSAPI\', ' +
      'serviceName: \'mongodb\', serviceHostname: \'karpov.madhacker.biz\' });';

    toolTest.stop = function() {
      print('No need to stop on Kerberos windows config. Test succeeded');
    };

    return toolTest;
  };
})();

var getCommonToolArguments = function() {
  return [
    '--username', 'luke.skywalker@MADHACKER.BIZ',
    '--password', AUTH_PASSWORD,
    '--host', 'karpov.madhacker.biz',
    '--authenticationDatabase', '$external',
    '--authenticationMechanism', 'GSSAPI',
    '--gssapiServiceName', 'mongodb',
    '--gssapiHostName', 'karpov.madhacker.biz'
  ];
};
