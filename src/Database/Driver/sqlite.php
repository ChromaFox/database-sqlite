<?php namespace CF\Database\Driver;

// We're going to default to MySQL/MariaDB because that's what I have
class sqlite extends \CF\Database\Driver
{
	public function getDatabaseTypeFor($type)
	{
		$types = [
			'int' => "INTEGER",
			'string' => "VARCHAR(120)",
			'text' => "TEXT",
			'list' => "VARCHAR(255)",
			'bool' => "TINYINT(1)",
			
			'primary' => " PRIMARY KEY",
			'auto' => " AUTOINCREMENT",
			'null' => [false => " NOT NULL", true => " NULL"],
			'default' => " DEFAULT "
		];
		
		if(is_array($type))
			$def = $types[$type[0]];
		else
			$def = $types[$type];
		
		if(is_subclass_of($def, "\CF\Database\Model", TRUE))
		{
			$info = $def::schema();
			
			// Get the type of the primary key
			$idCol = $type::getIDColumn($info['schema']);
			
			$type = $info['schema'][$idCol];
			unset($type['primary']);
			unset($type['auto']);
			
			return $this->getDatabaseTypeFor($type);
		}
		
		if(is_array($type))
		{
			if(isset($type['primary']))
				$def .= $types['primary'];
			if(isset($type['auto']))
				$def .= $types['auto'];
			if(isset($type['null']))
				$def .= $types['null'][$type['null']];
			if(isset($type['default']))
				$def .= $types['default']."'{$type['default']}'";
		}
		
		return $def;
	}
	
	function formatQuery($query)
	{
		$sql = call_user_func([$this, 'format'.$query->action], $query);
		
		if($query->whereValues)
		{
			$where = $this->formatWhere($query->whereValues);
			$sql['sql'] .= " WHERE " . $where['sql'];
			$sql['values'] = array_merge($sql['values'], $where['values']);
		}
		
		if($query->orderByValues)
			$sql['sql'] .= " ORDER BY " . $this->formatOrderBy($query->orderByValues);
		
		if($query->limitValues)
			$sql['sql'] .= " LIMIT " . $this->formatLimit($query->limitValues);
		
		if($query->groupColumn)
			$sql['sql'] .= " GROUP BY " . $query->groupColumn;
		
		return $sql;
	}
	
	function formatSelect($query)
	{
		if(is_array($query->columns))
			$formattedCols = implode(", ", $query->columns);
		else
			$formattedCols = $query->columns;
		
		if(is_array($query->join))
		{
			// Join-y fun!
			$sql = "SELECT {$formattedCols} FROM {$query->prefix}{$query->table}";
			if(isset($query->join["LEFT"]))
			{
				$left = $query->join["LEFT"];
				foreach($left as $leftTable => $on)
					$sql .= " LEFT JOIN {$query->prefix}{$leftTable} ON {$on}";
			}
		}
		else
			$sql = "SELECT {$formattedCols} FROM {$query->prefix}{$query->table}";
		
		return ['sql' => $sql, 'values' => []];
	}
	
	function formatUpdate($query)
	{
		$sql = "UPDATE {$query->prefix}{$query->table} SET ";
		$values = [];
		
		foreach($query->queryValues as $col => $val)
		{
			if(is_array($val) && isset($val['raw']))
				$columns[] = "{$col} = {$val['raw']}";
			else
			{
				$columns[] = "{$col} = ?";
				$values[] = $val;
			}
		}
		
		$sql .= implode(", ", $columns);
		
		return ['sql' => $sql, 'values' => $values];
	}
	
	function formatInsert($query)
	{
		$columns = implode(', ', array_keys($query->queryValues));
		$values = array_values($query->queryValues);
		
		$placeholders = implode(', ', array_fill(0, count($query->queryValues), '?'));
		
		$sql = "INSERT INTO {$query->prefix}{$query->table} ({$columns}) VALUES ({$placeholders})";
		
		return ['sql' => $sql, 'values' => $values];
	}
	
	function formatDelete($query)
	{
		$sql = "DELETE FROM {$query->prefix}{$query->table}";
		
		return ['sql' => $sql, 'values' => []];
	}
	
	function formatCount($query)
	{
		return $this->formatSelect($query);
	}
	
	function formatCreateTable($query)
	{
		$coldefs = [];
		
		foreach($query->columns as $col => $def)
		{
			if(is_string($col))
				$coldefs[] = "{$col} {$def}";
			else
				$coldefs[] = $def;
		}
		
		$coldefs = implode(", ", $coldefs);
		$sql = "CREATE TABLE {$query->prefix}{$query->table} ({$coldefs})";
		if(!empty($query->queryValues))
			$sql .= " ENGINE = {$query->queryValues}";
		
		return ['sql' => $sql, 'values' => []];
	}
	
	function formatDropTable($query)
	{
		$sql = "DROP TABLE IF EXISTS {$query->prefix}{$query->table}";
		return ['sql' => $sql, 'values' => []];
	}
	
	function formatWhere($where, $combine = "AND")
	{
		$sql = "";
		
		$values = [];
		
		$clauses = [];
		
		foreach($where as $col => $value)
		{
			// remove comment/ID/uniquifier from column
			$commentPos = strpos($col, '#');
			if($commentPos !== false)
				$col = substr($col, 0, $commentPos);
			
			if(in_array($col, ["AND", "OR"]))
			{
				// Sub-where
				$subWhere = self::formatWhere($value, $col);
				
				$clauses[] = "({$subWhere['sql']})";
				$values = array_merge($values, $subWhere['values']);
			}
			else
			{
				if(is_array($value))
				{
					$value = array_unique($value);
					if(count($value) == 1)
						$value = $value[0];
				}
				
				$colVals = explode(' ', $col);
				$col = $colVals[0];
				if(isset($colVals[1]))
					$op = $colVals[1];
				else if(is_array($value))
					$op = "in";
				else
					$op = "=";
				
				if(is_array($value))
				{
					$values = array_merge($values, $value);
					$clauses[] = "{$col} {$op} (" .  implode(',', array_fill(0, count($value), '?')) . ")";
				}
				else
				{
					$values[] = $value;
					$clauses[] = "{$col} {$op} ?";
				}
			}
		}
		
		$sql = implode(" {$combine} ", $clauses);
		
		return ['sql' => $sql, 'values' => $values];
	}
	
	function formatOrderBy($columns)
	{
		$orders = [];
		foreach($columns as $col => $dir)
			$orders[] = "{$col} {$dir}";
		
		return implode(", ", $orders);
	}
	
	function formatLimit($limit)
	{
		if(is_array($limit))
			return "{$limit[0]}, {$limit[1]}";
		else
			return "{$limit}";
	}
}