class AnalyticViewAggregator {

    private var totalViews: Int = 0;

    fun add(view: Int): AnalyticViewAggregator {
        totalViews += view;
        return this
    }

    fun getTotalViews (): Int {
        return totalViews;
    }

}