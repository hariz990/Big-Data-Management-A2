package unit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import unit.comparator.PositionalQgramComparator;

public class QgramGenerator
{

  public int _len = 2;

  public QgramGenerator()
  {}

  public QgramGenerator(int length)
  {
    _len = length;
  }

  public HashSet< String > getQgramList( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      HashSet< String > resultList = new HashSet< String >();

      for ( int i = 1; i < _len; i++ )
      {
        String qgram = "";
        for ( int j = 0; j < i; j++ )
        {
          qgram += "$";
        }
        qgram += text.substring( 0, _len - i );
        resultList.add( qgram );
      }

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( qgram );
      }
      return resultList;
    }
  }


  public ArrayList< PositionalQgram > getPositionalQgramArrayList( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      ArrayList< PositionalQgram > resultList = new ArrayList< PositionalQgram >();

      for ( int i = 1; i < _len; i++ )
      {
        String qgram = "";
        for ( int j = 0; j < i; j++ )
        {
          qgram += "$";
        }
        qgram += text.substring( 0, _len - i );
        resultList.add( new PositionalQgram( qgram, (-i) ) );
      }

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( new PositionalQgram( qgram, i ) );
      }
      return resultList;
    }
  }



  public TreeSet< PositionalQgram > getPositionalQgramList( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeSet< PositionalQgram > resultList =
          new TreeSet< PositionalQgram >( new PositionalQgramComparator() );

      for ( int i = 1; i < _len; i++ )
      {
        String qgram = "";
        for ( int j = 0; j < i; j++ )
        {
          qgram += "$";
        }
        qgram += text.substring( 0, _len - i );
        resultList.add( new PositionalQgram( qgram, (-i) ) );
      }

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( new PositionalQgram( qgram, i ) );
      }
      return resultList;
    }
  }

  public TreeSet< PositionalQgram > getPositionalQgramListWithoutWildCard( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeSet< PositionalQgram > resultList =
          new TreeSet< PositionalQgram >( new PositionalQgramComparator() );
      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( new PositionalQgram( qgram, i ) );
      }
      return resultList;
    }
  }

  public TreeSet< String > getQgramListWithoutWildCard( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeSet< String > resultList = new TreeSet< String >();

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( qgram );
      }
      return resultList;
    }
  }
  
  
  
  public TreeSet< String > getQgramListWithoutLastQgram( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeSet< String > resultList = new TreeSet< String >();

      for ( int i = 0; i <= (text.length() - _len - 1); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( qgram );
      }
      return resultList;
    }
  }
  


  public HashSet< String > getQgramHashSet( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      HashSet< String > resultList = new HashSet< String >();

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( qgram );
      }
      return resultList;
    }
  }


  public ArrayList< String > getQgramTokenArrayList( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      ArrayList< String > resultList = new ArrayList< String >();

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( qgram );
      }
      return resultList;
    }
  }



  public ArrayList< PositionalQgram > getFirstMPositionalQgramArrayList( String text, int number )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      ArrayList< PositionalQgram > resultList = new ArrayList< PositionalQgram >();

      int qgramNumber = Math.min( (text.length() - _len + 1), number );

      for ( int i = 0; i < qgramNumber; i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( new PositionalQgram( qgram, i ) );
      }

      return resultList;
    }
  }


  public TreeSet< PositionalQgram > getFirstMPositionalQgramSet( String text, int number )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeSet< PositionalQgram > resultList = new TreeSet< PositionalQgram >();

      int qgramNumber = Math.min( (text.length() - _len + 1), number );

      for ( int i = 0; i < qgramNumber; i++ )
      {
        String qgram = text.substring( i, i + _len );
        resultList.add( new PositionalQgram( qgram, i ) );
      }

      return resultList;
    }
  }

  
  
  public TreeSet< String > getQgramTokenSetFromM( String text, int beginIndex )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {  
      int startIndex = beginIndex - 1;
      int endIndex = text.length() - _len;

      if ( text.length() >= ( beginIndex + _len ) )
      {
        TreeSet< String > resultList = new TreeSet< String >();
        for ( int i = startIndex; i <= endIndex; i++ )
        {
          String token = text.substring( i, i + _len );
          resultList.add( token );
        }

        return resultList;
      }
      else
      {
        return null;
      }          
    }
  }


  public TreeMap<String, Integer> getQgramTokenCountMap( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeMap<String, Integer> resultMap = new TreeMap<String, Integer>();

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String token = text.substring( i, i + _len );
        Integer count = resultMap.get( token );
        
        if ( count == null )
        {
          resultMap.put( token, 1 );         
        }
        else
        {
          resultMap.put( token, count + 1 );  
        }
      }
      return resultMap;
    }
  }
  
  public TreeMap<String, TreeSet<Integer>> getTokenPositionMap( String text )
  {
    if ( text.length() < _len )
    {
      return null;
    }
    else
    {
      TreeMap<String, TreeSet<Integer>> resultMap = new TreeMap<String, TreeSet<Integer>>();

      for ( int i = 0; i <= (text.length() - _len); i++ )
      {
        String token = text.substring( i, i + _len );
        TreeSet< Integer > posSet = resultMap.get( token );      
        
        if ( posSet == null )
        {
          posSet = new TreeSet< Integer >();
          posSet.add( i );
          resultMap.put( token, posSet ); 
        }
        else
        {
          posSet.add( i );
        }
      }
      return resultMap;
    }
  }
  
  
   
}
