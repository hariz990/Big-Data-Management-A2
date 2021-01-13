package unit.query;

public enum QueryType {

	// prefix query in a given spatial range
	PREFIX_RANGE,
	
	// prefix query in the whole data set
	PREFIX_ALL,
	
	// substring query in a given spatial range
	SUBSTRING_RANGE,
	
	// approximate prefix query in a given spatial range
	APPROXIMATE_PREFIX_RANGE,
	
	// approximate substring query in a given spatial range
	APPROXIMATE_SUBSTRING_RANGE,
	
	// prefix selectivity estimation in the whole data set
	PREFIX_SELECTIVITY,
	
	// substring selectivity estimation in a given spatial range
	SUBSTRING_SELECTIVITY;
	
	
	/**
	 * 
	 * @param option
	 * @return whether this query contains the result of the other query 
	 */
	public QueryType getNextRelaxation(QueryType type)
	{
		if ( type == PREFIX_RANGE ) 
		{
		  return PREFIX_ALL;
		} 
		else if ( type == PREFIX_ALL ) 
		{
			return SUBSTRING_RANGE;
		} 
		else if ( type == SUBSTRING_RANGE ) 
		{
			return APPROXIMATE_PREFIX_RANGE;
		}
		else if ( type == APPROXIMATE_PREFIX_RANGE ) 
		{
			return APPROXIMATE_SUBSTRING_RANGE;
		}
		else if ( type == APPROXIMATE_SUBSTRING_RANGE ) 
		{
			return APPROXIMATE_SUBSTRING_RANGE;
		}		
		else 
		{
			return this;
		}		
	}
	
	
	   public boolean contains(QueryType option)
	    {
	        if ( this == PREFIX_RANGE && option == PREFIX_RANGE ) 
	        {
	            return true;
	        } 
	        else if ( this == PREFIX_RANGE ) 
	        {
	            return false;
	        } 
	        else if ( this == PREFIX_ALL && ( option == PREFIX_RANGE || option == PREFIX_ALL ) ) 
	        {
	            return true;
	        }
	        else if ( this == PREFIX_ALL ) 
	        {
	            return false;
	        }
	        else if ( this == SUBSTRING_RANGE && ( option == PREFIX_RANGE || option == SUBSTRING_RANGE ) ) 
	        {
	            return true;
	        }
	        else if ( this == SUBSTRING_RANGE ) 
	        {
	            return false;
	        }
	        else if ( this == APPROXIMATE_PREFIX_RANGE && 
	                    ( option == PREFIX_RANGE || option == APPROXIMATE_PREFIX_RANGE ) ) 
	        {
	            return true;
	        } 
	        else if ( this == APPROXIMATE_PREFIX_RANGE ) 
	        {
	            return false;
	        }
	        else if ( this == APPROXIMATE_SUBSTRING_RANGE && option == PREFIX_ALL ) 
	        {
	            return false;
	        } 
	        else 
	        {
	            return true;
	        }       
	    }
	
	
	@Override
	public String toString()
	{
	  if ( this == PREFIX_RANGE ) {
	    return "SP";	  
	  }	else if ( this == PREFIX_ALL ) {
        return "SPR";
	  } else if ( this == SUBSTRING_RANGE ) {
        return "SS";  
	  } else if ( this == APPROXIMATE_PREFIX_RANGE ) {
        return "SAP";  
	  } else { 
	    return "SAS";
	  }
	}
	

  public int toInteger()
  {
    if ( this == PREFIX_RANGE )
    {
      return 1;
    }
    else if ( this == PREFIX_ALL )
    {
      return 2;
    }
    else if ( this == SUBSTRING_RANGE )
    {
      return 3;
    }
    else if ( this == APPROXIMATE_PREFIX_RANGE )
    {
      return 4;
    }
    else
    {
      return 5;
    }
  }
    
}
