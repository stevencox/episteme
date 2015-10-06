package org.episteme

import cc.factorie._
import cc.factorie.app.nlp.{ Document, Token }
import cc.factorie.app.chain.ChainModel
import cc.factorie.app.nlp.segment.{ DeterministicSentenceSegmenter, DeterministicTokenizer }
import cc.factorie.optimize.Trainer
import cc.factorie.model.{ DotFamilyWithStatistics2, Model }
import cc.factorie.variable.{
  LabeledCategoricalVariable, BinaryFeatureVectorVariable,
  CategoricalVectorDomain, CategoricalDomain, CategoricalVariable }
import cc.factorie.infer.InferByBPChain
import cc.factorie.infer.BP
import scala.xml.XML

// http://factorie.cs.umass.edu/usersguide/UsersGuide460Learning.html
// https://www.metacademy.org/roadmaps/rgrosse/bayesian_machine_learning
// https://en.wikipedia.org/wiki/Stochastic_gradient_descent
object Wikipedia extends App {
  implicit val random = new scala.util.Random(0)

  val c = new CategoricalDomain[String]()

  for (feature <- (0 until 1000).par) {
    // calling .index on a domain will add the category to the domain if it's not present,
    // and return its index. It is fine to do this from many threads at once.
    c.index(feature.toString)
  }

  object LabelDomain extends CategoricalDomain[String]

  class Label(val token: Token, s: String) extends LabeledCategoricalVariable(s) {
    def domain = LabelDomain
  }

  object FeaturesDomain extends CategoricalVectorDomain[String]
  class Features(val token: Token) extends BinaryFeatureVectorVariable[String] {
    def domain = FeaturesDomain
  }

  object model extends ChainModel[Label, Features, Token](
    LabelDomain,
    FeaturesDomain,
    l => l.token.attr[Features],
    l => l.token,
    t => t.attr[Label])

  def processText (documents : String) : Unit = {
//    val xml = XML.loadFile(documents) //"/projects/stars/var/wikipedia/enwiki-small.xml")
    //val text = (xml \\ "mediawiki" \\ "page" \\ "text").text
    val text = "really really great text"
    val document = new Document(documents) //text)
    DeterministicTokenizer.process(document)
    DeterministicSentenceSegmenter.process(document)

    document.tokens.foreach(t => t.attr += new Label(t, "A"))
    LabelDomain.index("B")
    document.tokens.foreach (t => {
      val features = t.attr += new Features(t)
      features += "W=" + t.string.toLowerCase
      features += "IsCapitalized=" + t.string(0).isUpper.toString
    })

    /*** 
     * "Base example for maximizing log likelihood. Works by calling inference and 
     * getting a summary, and using the information about the factor marginals in
     * the summary to compute the value and gradient."
     *   - http://factorie.cs.umass.edu/scaladocs/index.html#cc.factorie.optimize.LikelihoodExample
     */
    val example = new optimize.LikelihoodExample(
      labels = document.tokens.toSeq.map(_.attr[Label]),
      model  = model,
      infer  = InferByBPChain)

    println ("-- ** Adaptive Gradient Descent (AdaGrad) ---------------------------------------------------")
    val optimizer = new optimize.AdaGrad ()
    Trainer.batchTrain(model.parameters, Seq(example), optimizer = optimizer)


    val summary = InferByBPChain.infer(document.tokens.toSeq.map(_.attr[Label]), model)
    val marginal = summary.marginal (document.tokens.head.attr[Label])
    println (s" logZ: ${summary.logZ} marginal: ${marginal}")

    System.exit (0)
  }

  //processText ("/projects/stars/var/wikipedia/enwiki-small.xml")


  val xml = XML.loadFile("/projects/stars/var/wikipedia/enwiki-small.xml")
  (xml \\ "mediawiki" \\ "page" \\ "text").foreach { doc =>
    processText (doc.text)
    println (s" ----------> ${doc.text}")
  }
  System.exit (0)
/*
  def main (args: Array[String]): Unit = {
    processText (args (0))
    System.exit (0)
  }
 */
}
