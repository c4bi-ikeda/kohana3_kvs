<?php defined('SYSPATH') or die('No direct access allowed.');

class Kohana_KVS_Redis extends KVS {
	const	TYPE_NOT_FOUND	= 0;
	const	TYPE_STRING		= 1;
	const	TYPE_SET		= 2;
	const	TYPE_LIST		= 3;
	const	TYPE_ZSET		= 4;
	const	TYPE_HASH		= 5;

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
						'options'		=> array(
							Redis::OPT_SERIALIZER =>
											Redis::SERIALIZER_PHP,
						),
					));
		try {
			$this->_connection = new \Redis();

			if (!empty($this->_config['connection']['options'])) {
				foreach ($this->_config['connection']['options'] as
							$key => $val) {
					$this->_connection->setOption($key, $val);
				}
			}

			if ($persistent) {
				$this->_connection->pconnect(
									$hostname, $port, $timeout);
			}
			else {
				$this->_connection->connect(
									$hostname, $port, $timeout);
			}
		} catch (RedisException $e) {
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

		if (!$this->_connection->exists($key)) return null;

		$keytype = $this->_connection->type($key);
		switch ($keytype) {
		case self::TYPE_NOT_FOUND:
			return null;
			break;
		case self::TYPE_STRING:
			return $this->_connection->get($key);
			break;
		case self::TYPE_HASH:
			$vals = $this->_connection->hGetAll($key);
			break;
		default:
			throw new KVS_Exception(':key is unsupported type(=:type).',
						array(':key' => $key, ':type'	=> $keytype));
			return FALSE;
		}

		$serializer = Serialize::instance();
		// SERIALIZED キーがあれば Object/etc を
		// シリアライズしたものと判断する
		if (array_key_exists(Serialize::SERIALIZED, $vals)) {
			// Object 
			return $serializer->unserialize(
								$vals[Serialize::SERIALIZED]);
		}

		// Array であると判断する
		return $serializer->unflat($vals);
	}

	/**
	 * Find the multi key-value pair.
	 *
	 * @param	string		key-pattern
	 * @throws	KVS_Exception
	 * @return	mixed
	 */
	public function find($keypattern) {
		$this->connect();
		$keys = $this->_connection->keys($keypattern);

		$results = array();
		foreach ($keys as $idx => $key) {
			$results[$key] = $this->_connection->get($key);
		}
		return $results;
	}

	/**
	 * Set the single key-value pair.
	 *
	 * @param	string		key
	 * @param	mixed		values
	 * @param	int			Time-To-Live second(s)
	 * @throws	KVS_Exception
	 * @return	boolean
	 *
	 * value = scalar / string の場合のみ TTL 有効
	 *
	 */
	public function set($key, $value, $ttl = 0) {
		$this->connect();

		if (is_scalar($value) || is_string($value)) {
			// スカラと文字列はそのままsetする
			try {
				$ttl = intval($ttl);
				if ($this->_connection->exists($key)) {
					$this->delete($key);
				}

				if ($ttl > 0) {
					$this->_connection->setex($key, $ttl, $value);
				}
				else {
					$this->_connection->set($key, $value);
				}
			} catch (RedisException $e) {
				throw new KVS_Exception(':error',
							array(':error' => $e->getMessage()),
							$e->getCode());
				return FALSE;
			}
			return TRUE;
		}
		
		$serializer = Serialize::instance();
		if (is_array($value)) {
			// 配列ならばHash型で
			$hashval = $serializer->flat($value);
			try {
				if ($this->_connection->exists($key)) {
					$this->delete($key);
				}
				$this->_connection->hMset($key, $hashval);
			} catch (RedisException $e) {
				throw new KVS_Exception(':error',
							array(':error' => $e->getMessage()),
							$e->getCode());
				return FALSE;
			}
		}
		else {
			// それ以外(Objectとか)はシリアライズした上でHashへ
			$hashval = array(
				Serialize::SERIALIZED => $serializer->serialize($value),
			);
			try {
				if ($this->_connection->exists($key)) {
					$this->delete($key);
				}
				$this->_connection->hMset($key, $hashval);
			} catch (RedisException $e) {
				throw new KVS_Exception(':error',
							array(':error' => $e->getMessage()),
							$e->getCode());
				return FALSE;
			}
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
		} catch (RedisException $e) {
			throw new KVS_Exception(':error',
						array(':error' => $e->getMessage()),
						$e->getCode());
			return FALSE;
		}
		return TRUE;
	}
}
