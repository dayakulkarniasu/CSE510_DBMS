/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package btree;
import global.*;

/** KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
   /** key in the (key, data)
    */  
   public KeyClass key;
   /** data in the (key, data)
    */
   public DataClass data;
   
  /** Class constructor
   */
  public KeyDataEntry( Integer key, PageId pageNo) {
     this.key = new IntegerKey(key); 
     this.data = new IndexData(pageNo);
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( String key, PageId pageNo) {
     this.key = new StringKey(key); 
     this.data = new IndexData(pageNo);
  };
  
  /** Class constructor.
   */
  public KeyDataEntry( StringString key, PageId pageNo) {
      this.key = new StringStringKey(key); 
      this.data = new IndexData(pageNo);
   };

     /** Class constructor.
   */
  public KeyDataEntry( StringInteger key, PageId pageNo) {
      this.key = new StringIntegerKey(key); 
      this.data = new IndexData(pageNo);
   };

  /** Class constructor.
   */
  public KeyDataEntry( StringStringInteger key, PageId pageNo) {
      this.key = new StringStringIntegerKey(key); 
      this.data = new IndexData(pageNo);
   };

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, PageId pageNo) {

     data = new IndexData(pageNo); 
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());  
     else if ( key instanceof StringStringKey ) 
        this.key= new StringStringKey(((StringStringKey)key).getKey());    
     else if ( key instanceof StringIntegerKey ) 
        this.key= new StringIntegerKey(((StringIntegerKey)key).getKey());    
     else if ( key instanceof StringStringIntegerKey ) 
        this.key= new StringStringIntegerKey(((StringStringIntegerKey)key).getKey());      
  };


  /** Class constructor.
   */
  public KeyDataEntry( Integer key, MID mid) {
     this.key = new IntegerKey(key); 
     this.data = new LeafData(mid);
  };

  /** Class constructor.
   */
  public KeyDataEntry( String key, MID mid) {
     this.key = new StringKey(key); 
     this.data = new LeafData(mid);
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( StringString key, MID mid) {
      this.key = new StringStringKey(key); 
      this.data = new LeafData(mid);
   }; 

  /** Class constructor.
   */
  public KeyDataEntry( StringInteger key, MID mid) {
      this.key = new StringIntegerKey(key); 
      this.data = new LeafData(mid);
   }; 

  /** Class constructor.
   */
  public KeyDataEntry( StringStringInteger key, MID mid) {
      this.key = new StringStringIntegerKey(key); 
      this.data = new LeafData(mid);
   }; 

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, MID mid){
     data = new LeafData(mid); 
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());    
     else if ( key instanceof StringStringKey ) 
        this.key= new StringStringKey(((StringStringKey)key).getKey());    
     else if ( key instanceof StringIntegerKey ) 
        this.key= new StringIntegerKey(((StringIntegerKey)key).getKey());    
     else if ( key instanceof StringStringIntegerKey ) 
        this.key= new StringStringIntegerKey(((StringStringIntegerKey)key).getKey());    
  };




  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key,  DataClass data) {
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey()); 
     else if ( key instanceof StringStringKey ) 
        this.key= new StringStringKey(((StringStringKey)key).getKey()); 
     else if ( key instanceof StringIntegerKey ) 
        this.key= new StringIntegerKey(((StringIntegerKey)key).getKey()); 
     else if ( key instanceof StringStringIntegerKey ) 
        this.key= new StringStringIntegerKey(((StringStringIntegerKey)key).getKey()); 

     if ( data instanceof IndexData ) 
        this.data= new IndexData(((IndexData)data).getData());
     else if ( data instanceof LeafData ) 
        this.data= new LeafData(((LeafData)data).getData()); 
  }

  /** shallow equal. 
   *  @param entry the entry to check again key. 
   *  @return true, if entry == key; else, false.
   */
  public boolean equals(KeyDataEntry entry) {
      boolean st1,st2;

      if ( key instanceof IntegerKey )
         st1= ((IntegerKey)key).getKey().equals
                  (((IntegerKey)entry.key).getKey());
      else if ( key instanceof StringKey )
         st1= ((StringKey)key).getKey().equals
                  (((StringKey)entry.key).getKey());
      else if ( key instanceof StringStringKey )
         st1= ((StringStringKey)key).getKey().equals
                  (((StringStringKey)entry.key).getKey());
      else if ( key instanceof StringIntegerKey )
         st1= ((StringIntegerKey)key).getKey().equals
                  (((StringIntegerKey)entry.key).getKey());
      else 
         st1= ((StringStringIntegerKey)key).getKey().equals
                  (((StringStringIntegerKey)entry.key).getKey());
      

      if( data instanceof IndexData )
         st2= ( (IndexData)data).getData().pid==
              ((IndexData)entry.data).getData().pid ;
      else
         st2= ((MID)((LeafData)data).getData()).equals
                (((MID)((LeafData)entry.data).getData()));

  
      return (st1&&st2);
  }     
}

