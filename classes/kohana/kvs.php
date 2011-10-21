<?php defined('SYSPATH') or die('No direct access allowed.');

/**
 *
 * KVS connection wrapper/helper.
 *
 * @package		Kohana/KVS
 * @category	Base
 * @author		T.Ikeda @ PicoLabs
 * @copyright	(c) 2011 PicoLabs, Inc.
 */
abstract class Kohana_KVS {
	/**
	 * @var	string	Default instance name
	 */
	public static $default = 'default';

	/**
	 * @var	array	KVS instances
	 */
	public static $instances = array();

	/**
	 * Get a singleton KVS instance.
	 *
	 * @param	string		instance name
	 * @param	array		configuration parameters
	 * @return	KVS instance
	 */
	public static function instance($name = NULL,
									array $config = NULL) {
		if ($name === NULL) {
			$name = KVS::$default;
		}

		if (!isset(KVS::$instances[$name])) {
			if ($config === NULL) {
				$config = Kohana::$config->load('kvs')->$name;
			}

			if (!isset($config['type'])) {
				throw new Kohana_Exception(
						'KVS type not defined in :name configuration',
						array(':name' => $name));
			}

			$driver = 'KVS_'. ucfirst($config['type']);
			new $driver($name, $config);
		}

		return KVS::$instances[$name];
	}

	protected	$_instance;
	protected	$_connection;
	protected	$_config;

	/**
	 * Stores the KVS configuration locally and name the instance.
	 *
	 * [IMPORTANT!!] This method can't be accessed directly,
	 * you MUST use [KVS::instance].
	 *
	 * @return	void
	 */
	protected function __construct($name, array $config) {
		$this->_instance = $name;
		$this->_config = $config;

		KVS::$instances[$name] = $this;
	}

	/**
	 * Disconnect from the KVS when the object is destroyed.
	 *
	 *     // Destory the KVS instance
	 *     unset(KVS::$instances[(string)$db], $db);
	 * [IMPORTANT!!] Calling "unset($db)" is NOT enough to destroy
	 * the KVS, as it will still be stored in "KVS::$instances".
	 *
	 * @return	void
	 */
	final public function __destruct() {
		$this->disconnect();
	}

	/**
	 * Returns the KVS instance name.
	 *
	 * @return	string
	 */
	final public function __toString() {
		return $this->_instance;
	}

	/**
	 * Connect to the KVS.
	 *
	 * @throws	KVS_Exception
	 * @return	void
	 */
	abstract public function connect();

	/**
	 * Disconnect from the KVS.
	 *
	 * @return boolean
	 */
	public function disconnect() {
		unset(KVS::$instances[$this->_instance]);

		return TRUE;
	}

	/** --------------------------------------------------------
	 * Common Methods.
	 */

	/**
	 * Get the single key-value pair.
	 *
	 * @param	string		key
	 * @throws	KVS_Exception
	 * @return	mixed
	 */
	abstract public function get($key);

	/**
	 * Find the multi key-value pair.
	 *
	 * @param	string		keypattern
	 * @throws	KVS_Exception
	 * @return	mixed
	 */
	abstract public function find($keypattern);

	/**
	 * Set the single key-value pair.
	 *
	 * @param	string		key
	 * @param	mixed		values
	 * @throws	KVS_Exception
	 * @return	boolean
	 */
	abstract public function set($key, $value);

	/**
	 * Set the multi key-value pair.
	 *
	 * @param	array		key-value pairs
	 * @throws	KVS_Exception
	 * @return	boolean
	 */
	abstract public function sets(array $pairs);

	/**
	 * Remove the single key.
	 *
	 * @param	string		key
	 * @throws	KVS_Exception
	 * @return	boolean
	 */
	abstract public function delete($key);

	/** --------------------------------------------------------
	 * Magic Methods.
	 */
	public function __call($name, $arguments) {
		if (!method_exists($this->_connection, $name)) {
			throw new KVS_Exception('Unknown method :name on :driver driver.',
					array(':name'	=> $name,
						  ':driver'	=> get_class($this->_connection),
				));
		}

		try {
			return call_user_func(array($this->_connection, $name),
								  $arguments);
		} catch (Exception $e) {
			throw new KVS_Exception(':error',
					array(':error'	=> $e->getMessage()),
					$e->getCode());
		}
	}
}
