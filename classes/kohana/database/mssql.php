<?php defined('SYSPATH') or die('No direct script access.'); 
/**
* MsSQL database connection.
*
* @author     Kohana Team, xrado
*/
class Kohana_Database_MsSQL extends Database_PDO {
	
	public function query($type, $sql, $as_object)
	{
		// Make sure the database is connected
		$this->_connection or $this->connect();
		
		// Mssql specific
		$limit = $offset = NULL;

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

		if($limit || $offset)
		{
			if (!$offset) 
			{
				$sql = preg_replace("/^(SELECT|DELETE|UPDATE)\s/i", "$1 TOP " . $limit . ' ', $sql);
			} 
			else 
			{
				$orderby = stristr($sql, 'ORDER BY');

				if (!$orderby) 
				{
					$over = 'ORDER BY (SELECT 0)';
				} 
				else 
				{
					$over = preg_replace('/[^,\s]*\.([^,\s]*)/i', 'inner_tbl.$1', $orderby);
				}
				
				// Remove ORDER BY clause from $sql
				$sql = preg_replace('/\s+ORDER BY(.*)/', '', $sql);
				
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

			// Rethrow the exception
			throw $e;
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
				$result->setFetchMode(PDO::FETCH_CLASS, $as_object);
			}
			else
			{
				$result->setFetchMode(PDO::FETCH_CLASS, 'stdClass');
			}
			
			$result = $result->fetchAll();

			// Return an iterator of results
			return new Database_Result_Cached($result, $sql, $as_object);
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
		$data = $this->query(Database::SELECT,'SCOPE_IDENTITY() as id',FALSE)->current();
		return Arr::get($data,'id');
	}
	
	public function datatype($type)
	{
		static $types = array
		(
			'nvarchar'  => array('type' => 'string'),
			'ntext'     => array('type' => 'string'),
		);

		if (isset($types[$type]))
			return $types[$type];

		return parent::datatype($type);
	}
	
	public function list_tables($like = NULL)
	{
		if (is_string($like))
		{
			// Search for table names
			$result = $this->query(Database::SELECT, 'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '.$this->quote($like), false)->as_array();
		}
		else
		{
			// Find all table names
			$result = $this->query(Database::SELECT, 'SELECT * FROM INFORMATION_SCHEMA.TABLES', false)->as_array();
		}

		$tables = array();
		foreach ($result as $row)
		{
			// Get the table name from the results
			$tables[] = $row['TABLE_NAME'];
		}

		return $tables;
	}
	
	//@todo make this work
	public function list_columns($table, $like = NULL)
	{
		if (is_string($like))
		{
			$results = $this->query(Database::SELECT,'SELECT COLUMN_NAME AS Field, DATA_TYPE as Type, IS_NULLABLE as is_nullable FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME LIKE '.$this->quote($table), FALSE);
		}
		else
		{
			$results = $this->query(Database::SELECT,'SELECT COLUMN_NAME AS Field, DATA_TYPE as Type, IS_NULLABLE as is_nullable FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = '.$this->quote($table), FALSE);
		}

		$result = array();
		foreach ($results as $column)
		{
			// Make an associative array
			$result[$column['Field']] = $this->datatype($column['Type']);
		}

		return $result;
	}

	public function set_charset($charset){}

}
