package code
package lib

import net.liftweb.http.rest.RestHelper
import net.liftweb.json.JsonAST.JInt
import net.liftweb.json.JsonAST.JValue

object TaskRest extends RestHelper {
  
  serve("api" / "task" prefix {
    
    case "submit" :: Nil JsonGet _ => JInt(1)
    
  })
  
}
