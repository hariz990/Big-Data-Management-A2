package unit.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

import spatialindex.spatialindex.Region;
import stringverification.KMPMatch;
import unit.PositionalQgram;
import unit.QgramGenerator;


public class SpatialQuery {

  public int id;
  
  public QueryType _queryType;
  public String _queryText;

  public Region _queryRegion;
  public ArrayList<Region> _excludeRegionList;

  public int _tau;
  public static final double TAU_RATIO = 0.2;

  public int _minPosQgramMatch;
  public int _minQgramTokenMatch;
  
  
  public int _smallQValue = 2;
  public QgramGenerator _samllQgramGenerator;

  public int _largeQValue = 3;
  public QgramGenerator _largeQgramGenerator;

  public TreeSet<PositionalQgram> _positionalQgramSet;
  public TreeSet<PositionalQgram> _positionalQgramSetWithoutWildCard;
  public TreeSet<String> _qgramSetWithoutWildCard;
  public TreeMap<String, TreeSet<Integer>> _tokenPositionMap = null;
  public ArrayList<PositionalQgram> _positionalOneGramSet = null;
  public HashSet<Integer> _identicalStartPositions;

  public TreeMap<String, TreeSet< Integer >> _smallQgramTokenPositionMap;
  
  
  public double _areaEnlargeRatio = 2;
  public int _resultSizeThreshold = 2;
  
  
  
  
  public int getResultSizeThreshold()
  {
    return _resultSizeThreshold;
  }



  
  public void setResultSizeThreshold( int _resultSizeThreshold )
  {
    this._resultSizeThreshold = _resultSizeThreshold;
  }



  public double getAreaEnlargeRatio()
  {
    return _areaEnlargeRatio;
  }


  
  public void setAreaEnlargeRatio( double areaEnlargeRatio )
  {
    this._areaEnlargeRatio = areaEnlargeRatio;
  }

  
  // for large q value
  public TreeSet<PositionalQgram> _firstMPositionalQgramSet;
  public TreeSet<String> _largeQgramTokenSet;
  public TreeSet<String> _largeQgramTokenSetFromM;  
  public TreeMap<String, TreeSet< Integer >> _largeQgramTokenPositionMap;
  public TreeMap<String, TreeSet< Integer >> _largeQgramPrefixFilterTokenPositionMap;
  public int _positionUpperBound = -1;
  
  
  
  public KMPMatch _kmpMatch;

  public SpatialQuery(QueryType qType, String qText, Region qRegion, int gramLength) {
    this._queryType = qType;
    this._queryText = qText;
    this._queryRegion = qRegion;
    this._smallQValue = gramLength;
    this._tau = (int) (qText.length() * TAU_RATIO);

    genQgram();
    _kmpMatch = new KMPMatch(_queryText);
    _identicalStartPositions = this.getIdenticalStartPositions();
  }


  public SpatialQuery(QueryType qType, String qText, Region qRegion,
      ArrayList<Region> excludeRegionList, int gramLength) {
    this(qType, qText, qRegion, gramLength);
    this._excludeRegionList = excludeRegionList;
  }
  
  
  
  public SpatialQuery(QueryType qType, String qText, Region qRegion,
      int smallQValue, int largeQValue, int positionUpperBound) 
  {
    this._queryType = qType;
    this._queryText = qText;
    this._queryRegion = qRegion;
    this._smallQValue = smallQValue;
    this._largeQValue = largeQValue;
    this._positionUpperBound = positionUpperBound;
    this._tau = (int) (qText.length() * TAU_RATIO);
    
    _minPosQgramMatch = ( _queryText.length() - _smallQValue + 1 ) - ( _smallQValue * _tau );
    _minQgramTokenMatch = ( _queryText.length() - _largeQValue + 1 ) - ( _largeQValue * _tau );
        
    generateQgrams();
    _kmpMatch = new KMPMatch(_queryText);
  }
  
  public SpatialQuery(QueryType qType, String qText, Region qRegion, int smallQValue,
      int largeQValue, int positionUpperBound, int tau)
  {
    this._queryType = qType;
    this._queryText = qText;
    this._queryRegion = qRegion;
    this._smallQValue = smallQValue;
    this._largeQValue = largeQValue;
    this._positionUpperBound = positionUpperBound;
    this._tau = (int) (qText.length() * TAU_RATIO);

    _minPosQgramMatch = (_queryText.length() - _smallQValue + 1) - (_smallQValue * _tau);
    _minQgramTokenMatch = (_queryText.length() - _largeQValue + 1) - (_largeQValue * _tau);

    generateQgrams();
    _kmpMatch = new KMPMatch( _queryText );
    
    if ( this._tau > tau )
    {
      this._tau = tau;
      _minPosQgramMatch = (_queryText.length() - _smallQValue + 1) - (_smallQValue * _tau);
      _minQgramTokenMatch = (_queryText.length() - _largeQValue + 1) - (_largeQValue * _tau);
    }
  }
  
  


  public void changeEditDistance( int tau )
  {    
    this._tau = tau;
    _minPosQgramMatch = (_queryText.length() - _smallQValue + 1) - (_smallQValue * _tau);
    _minQgramTokenMatch = (_queryText.length() - _largeQValue + 1) - (_largeQValue * _tau);
  }
  
  
  public void changeToDefaultEditDistance()
  {    
    this._tau = (int) (_queryText.length() * TAU_RATIO);
    _minPosQgramMatch = (_queryText.length() - _smallQValue + 1) - (_smallQValue * _tau);
    _minQgramTokenMatch = (_queryText.length() - _largeQValue + 1) - (_largeQValue * _tau);
  }
  
  

  public SpatialQuery getRemainderQuery(SpatialQuery query2) {
    ArrayList<Region> excludeRegions = new ArrayList<Region>();
    if (query2._excludeRegionList != null) {
      excludeRegions.addAll(query2._excludeRegionList);
    }

    excludeRegions.add(query2._queryRegion);

    return new SpatialQuery(this._queryType, this._queryText, this._queryRegion, excludeRegions,
        this._smallQValue);
  }


  public boolean containsQuery(SpatialQuery query2) {
    if (this._queryType.contains(query2._queryType)) {
      if (this._queryRegion.contains(query2._queryRegion)) {
        if (this._queryText.startsWith(query2._queryText)) {
          return true;
        }
      }
    }
    return false;
  }


  public boolean isSubstring(String fullText) {
    return this._kmpMatch.match(fullText);
  }


  public boolean isSubstring(String fullText, TreeSet<Integer> startPositions) {
    return this._kmpMatch.match(fullText, startPositions);
  }


  private void genQgram() {
    _samllQgramGenerator = new QgramGenerator(_smallQValue);
    this._positionalQgramSet = _samllQgramGenerator.getPositionalQgramList(_queryText);
    this._positionalQgramSetWithoutWildCard =
        _samllQgramGenerator.getPositionalQgramListWithoutWildCard(_queryText);
    this._qgramSetWithoutWildCard = _samllQgramGenerator.getQgramListWithoutWildCard(_queryText);
  }


  private void generateQgrams() {
    _samllQgramGenerator = new QgramGenerator(_smallQValue);
    
//    _qgramSetWithoutWildCard = _samllQgramGenerator.getQgramListWithoutWildCard(_queryText);
    _qgramSetWithoutWildCard = _samllQgramGenerator.getQgramListWithoutLastQgram(_queryText);
    
    
    _largeQgramGenerator = new QgramGenerator(_largeQValue);
    _firstMPositionalQgramSet = generateFirstMPositionalQgramSet();
    _largeQgramTokenSet = generateLargeQgramTokenSet();
    _largeQgramTokenSetFromM = generateQgramTokenSetFromM();
    generateLargeQgramTokenPositionMap();
  }


  public TreeMap<String, TreeSet<Integer>> getQueryTokenPositionMap() {
    if (this._tokenPositionMap != null) {
      return this._tokenPositionMap.isEmpty() ? null : this._tokenPositionMap;
    } else {
      this._tokenPositionMap = new TreeMap<String, TreeSet<Integer>>();

      Iterator<String> tokenItr = this._qgramSetWithoutWildCard.iterator();
      Iterator<PositionalQgram> posQgramItr = this._positionalQgramSetWithoutWildCard.iterator();

      String token = null;
      PositionalQgram posQgram = null;
      int tokenComparedValue = 0;

      while (tokenItr.hasNext()) {
        token = tokenItr.next();

        if (posQgram != null) {
          tokenComparedValue = token.compareTo(posQgram._qgram);
        }

        while (tokenComparedValue >= 0) {
          // if the token is same
          if (tokenComparedValue == 0 && posQgram != null) {
            TreeSet<Integer> posSet = this._tokenPositionMap.get(token);
            if (posSet == null) {
              posSet = new TreeSet<Integer>();
              posSet.add(posQgram._pos);
              this._tokenPositionMap.put(token, posSet);
            } else {
              posSet.add(posQgram._pos);
            }
          }

          // get the next positional q-gram
          if (posQgramItr.hasNext()) {
            posQgram = posQgramItr.next();
            tokenComparedValue = token.compareTo(posQgram._qgram);
          } else {
            return this._tokenPositionMap.isEmpty() ? null : this._tokenPositionMap;
          }
        }
      }

      return this._tokenPositionMap.isEmpty() ? null : this._tokenPositionMap;
    }
  }



  public ArrayList<PositionalQgram> getPositionalOneGramSet() {
    if (_positionalOneGramSet != null) {
      return _positionalOneGramSet;
    } else {
      _positionalOneGramSet = new ArrayList<PositionalQgram>();

      // start from 1 since the one gram in position 0 is never used.
      // for ( int i = 0; i < _queryText.length(); i++ )

      for (int i = 1; i < _queryText.length() - 1; i++) {
        PositionalQgram oneGram = new PositionalQgram(_queryText.substring(i, i + 1), i);
        _positionalOneGramSet.add(oneGram);
      }
      return _positionalOneGramSet;
    }
  }


  public HashSet<Integer> getIdenticalStartPositions() {
    HashSet<Integer> result = new HashSet<Integer>();
    char startChar = _queryText.charAt(0);
    for (int i = 1; i < _queryText.length(); i++) {
      char character = _queryText.charAt(i);

      if (startChar == character) {
        result.add(i);
      }
    }

    Iterator<Integer> itr = result.iterator();
    while (itr.hasNext()) {
      int pos = itr.next();

      boolean isIdentical = true;
      for (int i = 1; i < (_queryText.length() - pos); i++) {
        if (_queryText.charAt(i) != _queryText.charAt(i + pos)) {
          isIdentical = false;
          break;
        }
      }

      if (!isIdentical) {
        itr.remove();
      }
    }

    return result.isEmpty() ? null : result;
  }

  private TreeSet<PositionalQgram> generateFirstMPositionalQgramSet()
  {
    return this._samllQgramGenerator.getFirstMPositionalQgramSet(_queryText, _positionUpperBound); 
  }

  private TreeSet<String> generateLargeQgramTokenSet()
  {
    return this._largeQgramGenerator.getQgramListWithoutWildCard(_queryText);
  }
  
  private TreeSet<String> generateQgramTokenSetFromM()
  {
    return this._largeQgramGenerator.getQgramTokenSetFromM( _queryText, _positionUpperBound );
  }

  public int getQueryTextLength()
  {
    return _queryText.length();
  }
  
  
  public void generateLargeQgramTokenPositionMap()
  {
    _smallQgramTokenPositionMap = this._samllQgramGenerator.getTokenPositionMap( _queryText );
    
    
    _largeQgramTokenPositionMap = this._largeQgramGenerator.getTokenPositionMap( _queryText );
    _largeQgramPrefixFilterTokenPositionMap = this._largeQgramGenerator.getTokenPositionMap( _queryText.substring( 0, _largeQValue * _tau + 1 ) );
  }
  
  @Override
  public String toString()
  {
    return "SpatialQuery [_queryType=" + _queryType + ", _queryText=" + _queryText
        + ", _queryRegion=" + _queryRegion + "]";
  }

  
}
