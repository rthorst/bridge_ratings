# Brige Ratings

Knowing a bridge player's skill is crucial to finding good partners and assigning credit, yet existing ways that bridge players are ranked are not very effective. This repository houses work-in-progress code to address this problem by evaluating alternative methods of ranking bridge players.

# Status

Currently, the repository ranks bridge players using an ELO-based ranking. However, evaluations are showing shockingly low (chance-level) prediction accuracy for this model. It is possible that there is an issue with the dataset, or, that ranking bridge players is more difficult than it appears.

# Future Work

Future work should diagnose why the ELO model (which is seemingly a very reasonable model) is predicting performance so poorly. This could be due to issues with the dataset (scripts/download_data.py) or model (scripts/train_elo.py). 

The most useful application for this ranking would be a partnership desk tool. Existing partnership desks are very ineffective, using only masterpoints (and usually pen-and-paper) to match players. Creating a web application for a virtual partnership desk, driven by a smarter ranking system, would be very useful.

