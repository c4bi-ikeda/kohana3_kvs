<?php defined('SYSPATH') or die('No direct access allowed.');

return array(
	'default'	=> array(
		'type'			=> 'redis',	// redis,tokyotyrant,memcache
		'connection'	=> array(
			'hostname'		=> '127.0.0.1',	// KVS server address
			'port'			=> '6379',		// KVS server port
			'persistent'	=> FALSE,
		),
	),
);

