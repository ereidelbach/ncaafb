library('ggplot2')
library("dplyr")
df_final <- left_join(df_head_sal, df_standings_red)

ggplot(df_final,aes(y=`Total Pay`, x=`W`)) + 
  geom_point(aes(col=`Conf`, size = `School Buyout AS OF 12/1/17`)) +
  geom_text(aes(label=`School`))

ggplot(df_final, aes(x=`W`, y=`Total Pay`, fill=`Conf`)) + geom_dotplot(binaxis = 'y', stackdir = 'center')

library("plotly")
plot_ly(df_final, x = ~Conf, y = ~`Total Pay`, text = ~School, type = 'scatter', mode = 'markers',
        sizes = c(10,50), 
        marker = list(opacity = 0.5, sizemode='diameter'))

lmfit = lm(`W` ~ `Total Pay`, data=df_final)

