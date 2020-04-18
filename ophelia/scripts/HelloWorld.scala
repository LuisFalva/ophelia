//package <SOURCE-PATH.NAME-PACKAGE>

object HelloWorld extends App{

  val cleanCryptoName = (majorCurrency: String) => {
    majorCurrency match {
      case "xrp" => "ripple in pesos"
      case "btc" => "bitcoin in pesos"
      case "tusd" => "true usd in pesos"
      case "eth" => "ether in pesos"
      case "bch" => "bitcoincash in pesos"
      case "ltc" => "litecoin in pesos"
      case _ => "Que onda JuanMa!"
    }
  }

  println(cleanCryptoName("xrp"))
  println(cleanCryptoName("btc"))
  println(cleanCryptoName("tusd"))
  println(cleanCryptoName("eth"))
  println(cleanCryptoName("bch"))
  println(cleanCryptoName("ltc"))
  println(cleanCryptoName(""))

}
