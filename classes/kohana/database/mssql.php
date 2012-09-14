<?php defined('SYSPATH') or die('No direct script access.'); 
/**
* MsSQL database connection.
*
* @author     Kohana Team, xrado
*/
class Kohana_Database_MsSQL extends Database_PDO {
	
	public function query($type, $sql, $as_object = FALSE, array $params = NULL)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();
		
		// Mssql specific
		if(preg_match("/OFFSET ([0-9]+)/i",$sql,$matches))
		{
			list($replace,$offset) = $matches;
			$sql = str_replace($replace,'',$sql);
		}

		if(preg_match("/LIMIT ([0-9]+)/i",$sql,$matches))
		{
			list($replace,$limit) = $matches;
			$sql = str_replace($replace,'',$sql);
		}

		if(isset($limit) || isset($offset))
		{
			if (!isset($offset)) 
			{
				$sql = preg_replace("/^(SELECT|DELETE|UPDATE)\s/i", "$1 TOP " . $limit . ' ', $sql);
			} 
			else 
			{
				$ob_count = (int)preg_match_all('/ORDER BY/i',$sql,$ob_matches,PREG_OFFSET_CAPTURE);

				if($ob_count < 1) 
				{
					$over = 'ORDER BY (SELECT 0)';
				} 
				else 
				{
					$ob_last = array_pop($ob_matches[0]);
					$orderby = strrchr($sql, $ob_last[0]);
					$over = preg_replace('/[^,\s]*\.([^,\s]*)/i', 'inner_tbl.$1', $orderby);
					
					// Remove ORDER BY clause from $sql
					$sql = substr($sql, 0, $ob_last[1]);
				}
				
				// Add ORDER BY clause as an argument for ROW_NUMBER()
				$sql = "SELECT ROW_NUMBER() OVER ($over) AS KOHANA_DB_ROWNUM, * FROM ($sql) AS inner_tbl";
			  
				$start = $offset + 1;
				$end = $offset + $limit;

				$sql = "WITH outer_tbl AS ($sql) SELECT * FROM outer_tbl WHERE KOHANA_DB_ROWNUM BETWEEN $start AND $end";
			}
		}

		if ( ! empty($this->_config['profiling']))
		{
			// Benchmark this query for the current instance
			$benchmark = Profiler::start("Database ({$this->_instance})", $sql);
		}

		try
		{
			$result = $this->_connection->query($sql);
		}
		catch (Exception $e)
		{
			if (isset($benchmark))
			{
				// This benchmark is worthless
				Profiler::delete($benchmark);
			}

			$errArr = $this->_connection->errorInfo();
			$resultTextError = $this->_connection->query( "select * from sys.messages where  language_id=1033 and message_id=".arr::get($errArr, 1, 0) )->fetchAll();
			
			// Convert the exception in a database exception
			throw new Database_Exception('[:code] :error ( :info )', array(
				':code' => $e->getCode(),
				':error' => $e->getMessage(),
				#':query' => $sql,
				':info' =>  arr::get($resultTextError[0], 'text')
			), $e->getCode());
		}

		if (isset($benchmark))
		{
			Profiler::stop($benchmark);
		}

		// Set the last query
		$this->last_query = $sql;

		if ($type === Database::SELECT)
		{
			// Convert the result into an array, as PDOStatement::rowCount is not reliable
			if ($as_object === FALSE)
			{
				$result->setFetchMode(PDO::FETCH_ASSOC);
			}
			elseif (is_string($as_object))
			{
				$result->setFetchMode(PDO::FETCH_CLASS, $as_object, $params);
			}
			else
			{
				$result->setFetchMode(PDO::FETCH_CLASS, 'stdClass');
			}

			$result = $result->fetchAll();

			// Return an iterator of results
			return new Database_Result_Cached($result, $sql, $as_object, $params);
		}
		elseif ($type === Database::INSERT)
		{
			// Return a list of insert id and rows created
			return array(
				$this->insert_id(),
				$result->rowCount(),
			);
		}
		else
		{
			// Return the number of rows affected
			return $result->rowCount();
		}
	}
	
	public function insert_id()
	{
		$table = preg_match('/^insert\s+into\s+(.*?)\s+/i',$this->last_query,$match) ? arr::get($match,1) : NULL;
		if (!empty($table)) $query = 'SELECT IDENT_CURRENT(\'' . $this->quote_identifier($table) . '\') AS insert_id';
		else $query = 'SELECT SCOPE_IDENTITY() AS insert_id';

		$data = $this->query(Database::SELECT,$query,FALSE)->current();
		return Arr::get($data,'insert_id');
	}
	
	public function datatype($type)
	{
		static $types = array
		(
			'nvarchar'  => array('type' => 'string'),
			'ntext'     => array('type' => 'string'),
			'tinyint'   => array('type' => 'int', 'min' => '0', 'max' => '255'),
		);

		if (isset($types[$type]))
			return $types[$type];

		return parent::datatype($type);
	}
	
	public function begin($mode = NULL)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		return $this->_connection->beginTransaction();
	}

	public function commit()
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		return $this->_connection->commit();
	}

	public function rollback()
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();

		return $this->_connection->rollBack();
	}
	
	public function list_tables($like = NULL)
	{
		if (is_string($like))
		{
			// Search for table names
			$result = $this->query(Database::SELECT, 'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '.$this->quote($like), FALSE)->as_array();
		}
		else
		{
			// Find all table names
			$result = $this->query(Database::SELECT, 'SELECT * FROM INFORMATION_SCHEMA.TABLES', FALSE)->as_array();
		}

		$tables = array();
		foreach ($result as $row)
		{
			// Get the table name from the results
			$tables[] = $row['TABLE_NAME'];
		}

		return $tables;
	}
	
	public function list_columns($table, $like = NULL, $add_prefix = TRUE)
	{
		if (is_string($like))
		{
			$results = $this->query(Database::SELECT,'SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME LIKE '.$this->quote($table), FALSE);
		}
		else
		{
			$results = $this->query(Database::SELECT,'SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = '.$this->quote($table), FALSE);
		}

		$result = array();
		foreach ($results as $row)
		{
			list($type, $length) = $this->_parse_type($row['DATA_TYPE']);

			$column = $this->datatype($type);

			$column['column_name']      = $row['COLUMN_NAME'];
			$column['column_default']   = $row['COLUMN_DEFAULT'];
			$column['data_type']        = $type;
			$column['is_nullable']      = ($row['IS_NULLABLE'] == 'YES');
			$column['ordinal_position'] = $row['ORDINAL_POSITION'];
			
			if($row['CHARACTER_MAXIMUM_LENGTH'])
			{
				$column['character_maximum_length'] = $row['CHARACTER_MAXIMUM_LENGTH'];
			}
			
			$result[$row['COLUMN_NAME']] = $column;
		}

		return $result;
	}

	public function set_charset($charset){}

}
