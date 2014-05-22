<?php defined('SYSPATH') or die('No direct access allowed.');

class Kohana_KVS_Memcached extends KVS {
	/**
	 * Connect to the KVS.
	 *
	 * @throws	KVS_Exception
	 * @return	void
	 */
	public function connect() {
		if ($this->_connection) return ;

		extract($this->_config['connection'] + array(
						'hostname'		=> '127.0.0.1',
						'port'			=> '6379',
						'persistent'	=> FALSE,
						'timeout'		=> 30,
						'weight'		=> 10,
						'options'		=> array(),
					));
		try {
			$this->_connection = new \Memcache();
			$this->_connection->addServer(
								$hostname, $port, $persistent,
								$weight, $timeout);
		} catch (Exception $e) {
			$this->_connection = NULL;
			throw new KVS_Exception(':error',
						array(':error' => $e->getMessage()),
						$e->getCode());
		}
	}

	/**
	 * Disconnect from the KVS.
	 *
	 * @return boolean
	 */
	public function disconnect() {
		try {
			$status = TRUE;

			if (is_resource($this->_connection)) {
				$this->_connection->close();

				unset($this->_connection);
				$this->_connection = NULL;

				parent::disconnect();
			}
		} catch (Exception $e) {
			$status = !is_resource($this->_connection);
		}

		return $status;
	}

	/**
	 * Get the single key-value pair.
	 *
	 * @param	string		key
	 * @throws	KVS_Exception
	 * @return	mixed
	 */
	public function get($key) {
		$this->connect();

		$serializer = Serialize::instance();
		$val = $this->_connection->get($key);
		if (!$val) return $val;

		// 問答無用でアンシリアライズ
		return $serializer->unserialize($val);
	}

	/**
	 * Set the single key-value pair.
	 *
	 * @param	string		key
	 * @param	mixed		values
	 * @throws	KVS_Exception
	 * @return	boolean
	 */
	public function set($key, $value, $ttl = 0) {
		$this->connect();
		$ttl = intval($ttl);

		$serializer = Serialize::instance();
		try {
			$this->_connection->set($key,
						$serializer->serialize($value), 0, $ttl);
		} catch (Exception $e) {
			throw new KVS_Exception(':error',
						array(':error' => $e->getMessage()),
						$e->getCode());
			return FALSE;
		}
		return TRUE;
	}

	/**
	 * Set the multi key-value pair.
	 *
	 * @param	array		key-value pairs
	 * @throws	KVS_Exception
	 * @return	boolean
	 */
	public function sets(array $pairs, $ttl = 0) {
		$this->connect();
		try {
			foreach ($pairs as $key => $val) {
				$status = $this->set($key, $val, $ttl);
			}
		} catch (Exception $e) {
			throw $e;
			return FALSE;
		}
		return $status;
	}

	/**
	 * Remove the single key.
	 *
	 * @param	string		key
	 * @throws	KVS_Exception
	 * @return	boolean
	 */
	public function delete($key) {
		$this->connect();
		try {
			$this->_connection->delete($key);
		} catch (Exception $e) {
			throw new KVS_Exception(':error',
						array(':error' => $e->getMessage()),
						$e->getCode());
			return FALSE;
		}
		return TRUE;
	}
}
