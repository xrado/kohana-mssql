**Kohana PDO MSSql driver with emulation of LIMIT and OFFSET**

requirements
------------

- MSSql Server 2005 (or newer)
- php pdo mssql driver
- freetds


bootstrap.php
-------------

	Kohana::modules(array(
		...
		'kohana-mssql'   => MODPATH.'kohana-mssql',
		'database'       => MODPATH.'database',
		...
	));

(order is important)


config/database.php
-------------------

	return array
	(
		'default' => array(
			'type'       => 'mssql',
			'connection' => array(
				/**
				 * The following options are available for PDO:
				 *
				 * string   dsn
				 * string   username
				 * string   password
				 * boolean  persistent
				 * string   identifier
				 */
				'dsn'        => 'dblib:host=hostname;dbname=database',
				'username'   => 'test',
				'password'   => 'test',
				'persistent' => FALSE,
			),
			'table_prefix' => '',
			'charset'      => FALSE,
			'caching'      => FALSE,
			'profiling'    => TRUE,
		),
	);


example usage
-------------

	DB::query(Database::SELECT, 'SELECT * FROM table OFFSET 10 LIMIT 20');
	DB::select('*')->from('table')->limit(10)->offset(10)->execute();
	ORM::factory('model')->offset(30)->limit(15)->find_all();
