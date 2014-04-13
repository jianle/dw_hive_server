package code
package lib

import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonAST.JInt

object HelloRest extends RestHelper {
  
  serve("api" :: Nil prefix {
    
    case "count" :: Nil JsonGet _ => JInt(100)
    
  })
  
}
