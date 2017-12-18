package common

/**
  * Created by hry on 2017/10/20.
  */
class MailUtil {
  def sendMail(email:String) ={
    val cmd = "echo \"hi, \" "  + " | mail -s \"pixel problems\" "+ email
    val commands = Array("/bin/bash", "-c", cmd)

    try {
      Runtime.getRuntime().exec(commands);
    } catch  {
      case e: Exception =>
      e.printStackTrace();
    }
  }

}
object MailUtil extends MailUtil