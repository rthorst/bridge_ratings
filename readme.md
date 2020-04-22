# Brige Ratings

Knowing a bridge player's skill is crucial to finding good partners and assigning credit, yet existing ways that bridge players are ranked are not very effective. This repository houses work-in-progress code to address this problem by evaluating alternative methods of ranking bridge players.

# Status

Currently, the repository ranks bridge players using an ELO-based ranking. However, evaluations show very low prediction accuracy (spearman rho ~= 0.02). Further, the ELO rankings look visually only somewhat reasonable; some of the top players are indeed good players, but some below average players are ranked very highly.


# Future Work

The most immediate future work should improve the model. My belief is that there are no issues with the code and model; thus, a better model is needed.

This also should be developed around a specific application. The most promising would be a virtual "partnership desk," since the largest need for rankings is when people are finding new partners. An ELO-based partnership desk would help people find good partners.
