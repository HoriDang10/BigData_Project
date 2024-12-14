// DOM Elements
const recommendedSongs = document.getElementById("recommendedSongs");
const reloadRecommendationsBtn = document.getElementById("reloadRecommendationsBtn");

// Fetch Initial Recommendations (Top 10 Most Popular Songs)
function fetchInitialRecommendations() {
  recommendedSongs.innerHTML = "<li>Loading...</li>"; // Show loading indicator

  fetch("/recommend_initial")
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        recommendedSongs.innerHTML = `<li>${data.error}</li>`;
        return;
      }

      // Display recommended songs
      const recommendations = data.recommended_songs || [];
      if (recommendations.length === 0) {
        recommendedSongs.innerHTML = "<li>No recommendations available.</li>";
        return;
      }

      recommendedSongs.innerHTML = ""; // Clear previous recommendations
      recommendations.forEach((song) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <span>${song.title}</span>
          <span>${song.artist}</span>
          <span>Popularity: ${song.popularity}</span>
        `;
        recommendedSongs.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error fetching initial recommendations:", error);
      recommendedSongs.innerHTML = "<li>Error loading recommendations.</li>";
    });
}

// Fetch Recommendations Based on Playlist
function fetchRecommendations() {
  // Get the playlist songs
  const playlistSongs = [...likedSongs.querySelectorAll("li")].map((li) => {
    const title = li.querySelector("span:nth-child(1)").textContent;
    const artist = li.querySelector("span:nth-child(2)").textContent;
    return { title, artist };
  });

  // If playlist is empty, fetch initial recommendations
  if (playlistSongs.length === 0) {
    fetchInitialRecommendations();
    return;
  }

  // Send playlist to the backend
  fetch("/recommend_based_on_playlist", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ playlist: playlistSongs }), // Send the playlist to the backend
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.error) {
        recommendedSongs.innerHTML = `<li>${data.error}</li>`;
        return;
      }

      // Display recommended songs
      const recommendations = data.recommended_songs || [];
      if (recommendations.length === 0) {
        recommendedSongs.innerHTML = "<li>No recommendations available.</li>";
        return;
      }

      recommendedSongs.innerHTML = ""; // Clear previous recommendations
      recommendations.forEach((song) => {
        const li = document.createElement("li");
        li.innerHTML = `
          <span>${song.title}</span>
          <span>${song.artist}</span>
          <span>Popularity: ${song.popularity}</span>
        `;
        recommendedSongs.appendChild(li);
      });
    })
    .catch((error) => {
      console.error("Error fetching recommendations:", error);
      recommendedSongs.innerHTML = "<li>Error loading recommendations.</li>";
    });
}

// Event Listener for Reload Recommendations Button
reloadRecommendationsBtn.addEventListener("click", fetchRecommendations);

// Initialize the App with Initial Recommendations
window.onload = fetchInitialRecommendations;
