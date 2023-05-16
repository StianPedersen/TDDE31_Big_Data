################################################################################
#                     ONLY ALLOWED PACKAGE                                     #
#                 install.packages("neuralnet")                                #
################################################################################

################################################################################
#                             TASK 1                                           #
################################################################################
library(neuralnet)
set.seed(1234567890)
Variable <- runif(500, 0, 10) #UNIFOTRM SAMPLE; 500 point in the range 1-10
mydata <- data.frame(Variable, Sin=sin(Variable)) #Create a DF with the columns X and SIN(x)
train_data <- mydata[1:25,] # Training
test_data <- mydata[26:500,] # Test
# Random initialization of the weights in the interval [-1, 1]
set.seed(1234567890)
winit <- unlist(c(runif(31, -1, 1)))# WeightInit
  nn <- neuralnet(Sin ~ Variable,
                  data = train_data,
                  hidden = c(10), #One hidden layer with 10 nodes
                  startweights = winit
                  ) #Neuralnetwork model.
# Plot of the training data (black), test data (blue), and predictions (red)
plot(train_data, cex=2, xlim = c(0,14),
   main = "Approximation of sin value using nn-model",
   xlab = "Input value")
points(test_data, col = "blue", cex=1, pch = 16)
points(test_data[,1],predict(nn,test_data), col="red", cex=1, pch = 16)
legend("bottomright",
     legend=c("Training data","Test data", "NN prediction"),
     pch=c(
       1,16,16), col = c("black","blue","red"), title="Datapoints")


################################################################################
#                             TASK 2                                           #
################################################################################
linear <- function(x) {
  x
  }
relu <- function(x) { #APPROXIMATION OF RELU BCUS LIFE IS PAIN.
  x / (1 + exp(-2 * 1 * x))
  }
softplus <- function(x) {
  log(1 + exp(x))
  }

nn_linear <- neuralnet(Sin ~ Variable,
                 data = train_data,
                 hidden = c(10),
                 startweights = winit,
                 act.fct = linear) #Train nn with linear activation func


nn_ReLU <- neuralnet(Sin ~ Variable,
                 data = train_data,
                 hidden = c(10),
                 startweights = winit,
                 act.fct = relu) #Train a NN with relu approx as activation func

nn_soft <- neuralnet(Sin ~ Variable,
                data = train_data,
                hidden = c(10),
                startweights = winit,
                act.fct = softplus,
                threshold = 0.002) #Train NN with Softplus
# Plot of the linear NN
plot(train_data, cex=2,xlim = c(0,14),
     main = "NN with linear activation function")
points(test_data, col = "blue", cex=1)
points(test_data[,1],predict(nn_linear,test_data), col="red", cex=1)
legend("bottomright", inset=c(-0.0,0),
       legend=c("Training data","Test data", "NN prediction"),
       pch=c(1,16,16), col = c("black","blue","red"), title="Datapoints")

#Plot of ReLU pred
plot(train_data, cex=2,xlim = c(0,14),
     main = "NN with ReLU activation function")
points(test_data, col = "blue", cex=1)
points(test_data[,1],predict(nn_ReLU,test_data), col="green", cex=1)
legend("bottomright", inset=c(-0.0,0),
       legend=c("Training data","Test data", "NN prediction"),
       pch=c(1,16,16), col = c("black","blue","green"), title="Datapoints")

#Plot uing Softmax pred
plot(train_data, cex=2,xlim = c(0,14),
     main = "NN with Softmax activation function")
points(test_data, col = "blue", cex=1)
points(test_data[,1],predict(nn_soft,test_data), col="yellow", cex=1)
legend("bottomright", inset=c(-0.0,0),
       legend=c("Training data","Test data", "NN prediction"),
       pch=c(1,16,16), col = c("black","blue","yellow"), title="Datapoints")


################################################################################
#                             TASK 3                                           #
################################################################################
set.seed(1234567890)
Variable2 <- runif(500, 0, 50) #Uniform sample of 500 points in the interval 1-50
mydata2 <- data.frame(Variable = Variable2, Sin=sin(Variable2)) #Create df with x and sin(x)
#plot and predict using the NN with logistic sigmoid. (From task one)
plot(mydata2, col = "black", cex=1)
points(mydata2[,1], predict(nn,mydata2),  col = "blue", cex=1)
plot(mydata2, col = "black", cex=1, ylim = c(-12,1),
     main = "Sample ponits 0 < x < 50, prediction using first NN model")
points(mydata2[,1], predict(nn,mydata2), col = "blue", cex=1)
legend("bottomleft",
       legend=c("Test data", "NN prediction"),
       pch=c(1,1), col = c("black","blue"), title="Datapoints")

################################################################################
#                             TASK 4                                           #
################################################################################

nn$weights #USED To EXPLAIN WHY IT CONVERGES TO -12

################################################################################
#                             TASK 5                                           #
################################################################################

set.seed(1234567890)
Variable <- runif(500, 0, 10) #Sample again uniformly 500 points, interval 1-10
mydata <- data.frame(Variable, Sin=sin(Variable)) #Create a DF with the columns X and SIN(x)
nn_var_from_sin <- neuralnet(Variable ~ Sin,
                data = mydata,
                hidden = c(10),
                startweights = winit,
                threshold = 0.02) #train NN mod, X FROM SIN INSTEAD OF SIN FROM X
#plot the resulting prediction
plot(x = train_data[,2], y = train_data[,1],
     col = "black", ylim = c(0,10),
     xlab = "Sinus value",
     ylab = "X value", pch = 6,
     main = "X-value predicted from sinus")
points(train_data[,2],predict(nn_var_from_sin, train_data), col = "blue", pch = 20)
legend("topleft",
       legend=c("Test data", "NN prediction"),
       pch=c(6,20), col = c("black","blue"), title="Datapoints",
       inset=c(0,0))
