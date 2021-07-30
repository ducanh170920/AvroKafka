Bài 1:
      
GET dantri_anhhd25/_search
{
 "query": {
   "bool": {
      "must": [
        {  
          "range": {
            "time": {
              "gte": 1356998400,
              "lte": 1388534400
            }
          }
        }
        ,
        {
           "multi_match": {
            "query": "an toàn, đường sắt,đường bộ",
            "fields": ["title","description","content"]
          }
        }
      ]
   }    
 }
}

Bài 2:
  GET dantri_anhhd25/_search
{
  "query": {
    "bool": {
      
      "must_not": [
        {
          "match_phrase": {
            "description": "Hà Nội"
          }
        }
      ],
      "must": [
        {
          "prefix": {
            "title.keyword": {
              "value": "Hà Nội"
            }
          }
        }
      ]
    }
  }
}   
Bài 3
file: main.py tách từ và đẩy ra file
      SearchElastic tìm từ theo gợi ý nhập vào
