import java.io.{File, PrintWriter}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Community
{


  def createCombinations(userList: List[Int]): List[String] =
  {
    return userList.combinations(2).map(x=>x.sorted.mkString("-")).toList
  }


  def makeEdges(edgeStr: String): List[(String, String)] =
  {
    val verts=edgeStr.split('-')
    return List((verts(0),verts(1)),(verts(1),verts(0)))
  }



  def getDepth_nodesMap_n_maxDepth(broadcastNodeVsAdjnodesMap: Broadcast[mutable.Map[Int, Set[Int]]], root: Int,level:Int): (Int, mutable.HashMap[Int, Set[Int]]) =
  {
    var depth_nodesToVisitQueue=new ListBuffer[(Int,Int)]
    val temp=(0,root)
    var VisitedSet=new mutable.HashSet[Int]

    depth_nodesToVisitQueue+=temp
    VisitedSet+=root

    var levelVsNodesMap= new mutable.HashMap[Int,Set[Int]]()


    var tempList=new ListBuffer[Int]
    var prevDepth=0
    while(depth_nodesToVisitQueue.size>0)
      {
        val curr=depth_nodesToVisitQueue.remove(0)
        val curLevel=curr._1
        val curNode=curr._2

//        println(curLevel,curNode)
        if(curLevel!=prevDepth)
          {
            levelVsNodesMap(prevDepth)=tempList.toSet
            tempList.clear()
            prevDepth=curLevel
          }
        tempList+=curNode

        val children=broadcastNodeVsAdjnodesMap.value(curr._2).filter(n=>VisitedSet.contains(n)==false)
        VisitedSet=VisitedSet++children
        for (chi <- children)
        {
          val nextNode=(curLevel+1,chi)
          depth_nodesToVisitQueue+=nextNode
        }
      }
    levelVsNodesMap(prevDepth)=tempList.toSet
    return (prevDepth,levelVsNodesMap)

  }

  def getEdgeWeights(broadcastNodeVsAdjnodesMap: Broadcast[mutable.Map[Int, Set[Int]]], maxLevel_levelVsNodesMap: (Int, mutable.HashMap[Int, Set[Int]])): mutable.Seq[(String, Double)] =
  {
    val maxDepth=maxLevel_levelVsNodesMap._1
    val levelVsNodeSetMap=maxLevel_levelVsNodesMap._2
    var nodeVsWeightMap=new mutable.HashMap[Int,Double]()
    var curDepth=maxDepth
    var returnEdgeVsWeightList=new ListBuffer[(String,Double)]
    while(curDepth>0)
      {
        for(curNodeAtKthLevel<-levelVsNodeSetMap(curDepth))
          {
            var EdgeList=new ListBuffer[String]
            var parentList=new ListBuffer[Int]
            val kConnections=broadcastNodeVsAdjnodesMap.value(curNodeAtKthLevel)

            for(kMinus1thLevelNode<-levelVsNodeSetMap(curDepth-1))
            {
                if(kConnections.contains(kMinus1thLevelNode))
                  {
                    parentList+=kMinus1thLevelNode

                    if(curNodeAtKthLevel<kMinus1thLevelNode)
                      {
                        EdgeList+=curNodeAtKthLevel.toString+"-"+kMinus1thLevelNode.toString
                      }
                    else
                      {
                        EdgeList+=kMinus1thLevelNode.toString+"-"+curNodeAtKthLevel.toString
                      }

                  }
            }

            val vertexWeight=nodeVsWeightMap.getOrElse(curNodeAtKthLevel,1.0)
            val flowWeight=(vertexWeight/parentList.size)
            for (parentNode <- parentList) 
            {
              val parentNodeWeight=nodeVsWeightMap.getOrElse(parentNode,1.0)
              nodeVsWeightMap(parentNode)=parentNodeWeight+flowWeight
            }
            EdgeList.foreach(e=>returnEdgeVsWeightList+=((e,flowWeight)))
          }
        curDepth=curDepth-1
      }
    return returnEdgeVsWeightList
  }

  def getBetweeness(broadcastNodeVsAdjnodesMap: Broadcast[mutable.Map[Int, Set[Int]]], root: Int): mutable.Seq[(String, Double)] =
  {
    val maxLevel_levelVsNodesMap=getDepth_nodesMap_n_maxDepth(broadcastNodeVsAdjnodesMap,root,0)
//        println("root",root)
    return getEdgeWeights(broadcastNodeVsAdjnodesMap,maxLevel_levelVsNodesMap)
  }

  def calculateMod(broadcastUpdatedNode: Broadcast[mutable.Map[Int, Set[Int]]], curNode:Int,M_no_of_edges: Int):Double =
  {
    var accumulator=0.0

    for (keyNode <- broadcastUpdatedNode.value.keySet)
    {
      if(broadcastUpdatedNode.value(keyNode).contains(curNode))
        {
          val kCur_X_kJ_BY_2M=broadcastUpdatedNode.value(curNode).size*broadcastUpdatedNode.value(keyNode).size/(2*M_no_of_edges)
          accumulator+=(1-kCur_X_kJ_BY_2M)
        }
      else
        {
          val kCur_X_kJ_BY_2M=broadcastUpdatedNode.value(curNode).size*broadcastUpdatedNode.value(keyNode).size/(2*M_no_of_edges)
          accumulator+=(0-kCur_X_kJ_BY_2M)
        }
    }
    return accumulator
  }



  def getModularity(curN: Int, broadcastCluster: Broadcast[Set[Int]], broadcastNodeVsAdjnodesUpdatedGraph: Broadcast[mutable.Map[Int, Set[Int]]], totalClusterEdges_m: Int): Double =
  {

    val allclusterNodes=broadcastCluster.value

    var MAccum=0.0
    for ( innerNode<- allclusterNodes)
    {
      if(curN!=innerNode)
        {
          var Aij:Int=0
          if(broadcastNodeVsAdjnodesUpdatedGraph.value(curN).contains(innerNode))
            {
              Aij=1
            }
          var ki=broadcastNodeVsAdjnodesUpdatedGraph.value(curN).size
          var kj=broadcastNodeVsAdjnodesUpdatedGraph.value(innerNode).size

          val tempR=Aij-((ki*kj)/(2*totalClusterEdges_m))
          MAccum=MAccum+tempR
        }
    }
    return MAccum

  }

  def getClusters(allNodeVsConnectedNodelist: collection.Map[Int, Set[Int]]): mutable.Seq[mutable.HashSet[Int]] =
  {
    var visitedNodes=new mutable.HashSet[Int]
    var clusterList=new ListBuffer[mutable.HashSet[Int]]

    var toBeVisitedList=new ListBuffer[Int]

    for (node <- allNodeVsConnectedNodelist.keys)
    {
      if(!visitedNodes.contains(node))
        {
          toBeVisitedList+=node
          var clusterNodes=new mutable.HashSet[Int]

          while(!toBeVisitedList.isEmpty)
            {
              val cur=toBeVisitedList.remove(0)
//              println(visitedNodes.size)
              clusterNodes=clusterNodes+=cur
              toBeVisitedList=toBeVisitedList++allNodeVsConnectedNodelist(cur).filter(x=>visitedNodes.contains(x)==false)
              visitedNodes=visitedNodes++allNodeVsConnectedNodelist(cur)
            }
          clusterList+=clusterNodes
        }
    }
    return clusterList

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CommunityDetection").setMaster("local[*]")
    val sc = new SparkContext(conf)

//    var ratings_data = sc.textFile(args(0))
    var ratings_data = sc.textFile("input/ratings.csv")
    var header = ratings_data.first()
    ratings_data = ratings_data.filter(row => row != header)

    var allRatings = ratings_data.map(_.split(',') match { case Array(userID, movieID, rate, ts) => (movieID.toInt,List(userID.toInt))})

    val userToUser_mutualRatings=allRatings.reduceByKey((x1,x2)=>x1++x2).map(x=>createCombinations(x._2)).flatMap(x=>x).map(x=>(x,1)).reduceByKey((c1,c2)=>c1+c2).filter(x=>x._2>=75)

    val nodeVsAdjnodes=userToUser_mutualRatings.map(edgeStr=>makeEdges(edgeStr._1)).flatMap(x=>x).map(x=>(x._1.toInt,Set(x._2.toInt))).reduceByKey((v1,v2)=>v1++v2)
    val nodeVsAdjnodesMap=nodeVsAdjnodes.collectAsMap()



    val broadcastNodeVsAdjnodesMap = sc.broadcast(mutable.Map(nodeVsAdjnodesMap.toSeq:_*))

    val betweenNess=nodeVsAdjnodes.map(root=>getBetweeness(broadcastNodeVsAdjnodesMap,root._1)).flatMap(x=>x).reduceByKey((w1,w2)=>w1+w2).map(e_w=>(e_w._1.split("-")(0).toInt,e_w._1.split("-")(1).toInt,BigDecimal(e_w._2/2).setScale(1,BigDecimal.RoundingMode.DOWN).toDouble)).collect().toBuffer




        var allEdgeList=new ListBuffer[(Int,Int)]()
    new File("Betweeness.txt" ).delete()
        val pw = new PrintWriter(new File("Betweeness.txt" ))
        for(x<-betweenNess.sorted){
          pw.println(x)

          allEdgeList+=((x._1,x._2))
          allEdgeList+=((x._2,x._1))
        }
        pw.close()

        val allNodeVsConnectedNodelistMap=sc.parallelize(allEdgeList).map(x=>(x._1,Set(x._2))).reduceByKey((e1,e2)=>e1++e2).collectAsMap()

        val initialGraphCluster=sc.parallelize(allNodeVsConnectedNodelistMap.keys.toList)


          val broadcastCluster=sc.broadcast(initialGraphCluster.collect().toSet)
          val broadcastNodeVsAdjnodesUpdatedGraph=sc.broadcast(mutable.Map(allNodeVsConnectedNodelistMap.toSeq: _*))



    var updatingBTW=betweenNess.sortWith((x,y)=>x._3>y._3)

    val totalClusterEdges_m=updatingBTW.size


    var prevMod= 0.0
    var curMod=0.0


    var updatingNodeVsConnectedNodelistMap=collection.mutable.Map(sc.parallelize(allEdgeList).map(x=>(x._1,Set(x._2))).reduceByKey((e1,e2)=>e1++e2).collectAsMap().toSeq: _*)

    //      while(curMod>=prevMod)
    while(!updatingBTW.isEmpty && curMod>=prevMod)
    {
      val edgeToRemove = updatingBTW.remove(0)
      updatingNodeVsConnectedNodelistMap(edgeToRemove._1)=updatingNodeVsConnectedNodelistMap(edgeToRemove._1).filter(x=>x!=edgeToRemove._2)
      updatingNodeVsConnectedNodelistMap(edgeToRemove._2)=updatingNodeVsConnectedNodelistMap(edgeToRemove._1).filter(x=>x!=edgeToRemove._1)

      val temp=sc.broadcast(updatingNodeVsConnectedNodelistMap)

//      ## uncomment to update betweeness
      updatingBTW=sc.parallelize(updatingNodeVsConnectedNodelistMap.keys.toList).map(root=>getBetweeness(temp,root)).flatMap(x=>x).reduceByKey((w1,w2)=>w1+w2).map(e_w=>(e_w._1.split("-")(0).toInt,e_w._1.split("-")(1).toInt,BigDecimal(e_w._2/2).setScale(1,BigDecimal.RoundingMode.DOWN).toDouble)).collect().sortWith((x,y)=>x._3>y._3).toBuffer
//      ##

      var undivided_M=0.0

      val broadcastUpdatingNodeVsConnectedNodelistMap=sc.broadcast(updatingNodeVsConnectedNodelistMap)
      val remainingEdges=updatingBTW.size


      val broadcastCluster = sc.broadcast(updatingNodeVsConnectedNodelistMap.keys.toSet)

      undivided_M=undivided_M+updatingNodeVsConnectedNodelistMap.keys.map(curN=>getModularity(curN,broadcastCluster,broadcastUpdatingNodeVsConnectedNodelistMap,remainingEdges)).reduce((v1,v2)=>v1+v2)



      prevMod=curMod
      curMod=undivided_M/(2*remainingEdges)

//      #Tuning the modularity precision
      curMod=BigDecimal(curMod).setScale(1,BigDecimal.RoundingMode.DOWN).toDouble


    }

    val finClusters=getClusters(updatingNodeVsConnectedNodelistMap)
    new File("Clusters.txt" ).delete()
    val printCommunity = new PrintWriter(new File("Clusters.txt" ))

    for (elem <- finClusters.sortWith((x,y)=>x.size<y.size))
    {
      printCommunity.println(elem.toList.sorted)
    }
    printCommunity.close()
  }
}
