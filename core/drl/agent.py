import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import os

# Define the architecture of the Brain
class ActorCriticNetwork(nn.Module):
    def __init__(self, input_dim, num_links, power_levels=1, bw_levels=1):
        super(ActorCriticNetwork, self).__init__()
        
        # Shared Feature Extractor
        self.feature_extractor = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.LayerNorm(128), 
            nn.ReLU(),
            nn.Linear(128, 64),
            nn.ReLU()
        )
        
        # --- Actor Heads (Policy) ---
        
        # Head 1: Which Link to choose? (Discrete: 0 to num_links-1)
        self.link_head = nn.Sequential(
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, num_links),
            nn.Softmax(dim=-1)
        )
        
        # Head 2: What Tx Power to use? (Continuous: 0.0 to 1.0)
        self.power_head = nn.Sequential(
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, power_levels), 
            nn.Sigmoid() 
        )

        # Head 3: What Bandwidth to use? (Continuous: 0.0 to 1.0)
        self.bw_head = nn.Sequential(
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, bw_levels), 
            nn.Sigmoid() 
        )
        
        # --- Critic Head (Value Function) ---
        # Estimates "how good" the current state is. 
        self.value_head = nn.Sequential(
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )

    def forward(self, state):
        features = self.feature_extractor(state)
        
        link_probs = self.link_head(features)
        power_norm = self.power_head(features)
        bw_norm = self.bw_head(features)
        state_value = self.value_head(features)
        
        return link_probs, power_norm, bw_norm, state_value

class RLAgent:
    def __init__(self, state_dim, num_links=5, lr=1e-3, model_path="model_checkpoint.pth"):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.num_links = num_links
        self.model = ActorCriticNetwork(state_dim, num_links).to(self.device)
        self.optimizer = optim.Adam(self.model.parameters(), lr=lr)
        self.model_path = model_path
        
        # Bounds for Tx Power
        self.MIN_TX = 1.0
        self.MAX_TX = 30.0

        # Bounds for Bandwidth (0.5Mbps to 2.4Mbps)
        self.MIN_BW = 0.5
        self.MAX_BW = 2.4
        
        # Try to load existing model
        self.load_model()

    def get_action(self, state_vector, training=False, action_mask=None, noise_std=0.15):
        """
        Takes state, converts to tensor, returns decision (Link, Power, Bandwidth).
        """
        self.model.eval()
        state_tensor = torch.FloatTensor(state_vector).unsqueeze(0).to(self.device)
        
        with torch.no_grad():
            link_probs, power_norm, bw_norm, _ = self.model(state_tensor)
        
        # --- MASKING LOGIC FOR LINKS ---
        if action_mask is not None:
            mask_tensor = torch.FloatTensor(action_mask).unsqueeze(0).to(self.device)
            link_probs = link_probs * mask_tensor
            
            if link_probs.sum() > 0:
                link_probs = link_probs / link_probs.sum()
            else:
                link_probs = mask_tensor / mask_tensor.sum()

        # 1. Choose Link
        if training:
            dist = torch.distributions.Categorical(link_probs)
            link_action = dist.sample()
        else:
            link_action = torch.argmax(link_probs, dim=1)
            
        # 2. Choose Power & Bandwidth with Exploration Noise
        if training:
            # Θόρυβος για εξερεύνηση (Exploration) στο TX Power
            noise_pow = torch.randn_like(power_norm) * noise_std
            power_norm = torch.clamp(power_norm + noise_pow, 0.0, 1.0)
            
            # Θόρυβος για εξερεύνηση (Exploration) στο Bandwidth
            noise_bw = torch.randn_like(bw_norm) * noise_std
            bw_norm = torch.clamp(bw_norm + noise_bw, 0.0, 1.0)

        # Scale 0.0-1.0 back to physical ranges
        power_dbm = self.MIN_TX + (power_norm.item() * (self.MAX_TX - self.MIN_TX))
        bw_mbps = self.MIN_BW + (bw_norm.item() * (self.MAX_BW - self.MIN_BW))
        
        return int(link_action.item()), round(power_dbm, 1), round(bw_mbps, 1)

    def train_step(self, state, action_link, action_power, action_bw, reward):
        self.model.train()
        
        state_tensor = torch.FloatTensor(state).unsqueeze(0).to(self.device)
        reward_tensor = torch.FloatTensor([reward]).unsqueeze(0).to(self.device)
        
        # Forward pass
        link_probs, power_norm, bw_norm, state_value = self.model(state_tensor)
        
        # Calculate Loss
        critic_loss = (reward_tensor - state_value).pow(2)
        
        dist = torch.distributions.Categorical(link_probs)
        log_prob = dist.log_prob(torch.tensor(action_link).to(self.device))
        advantage = reward_tensor - state_value.detach()
        actor_loss_link = -log_prob * advantage
        
        total_loss = critic_loss + actor_loss_link
        
        self.optimizer.zero_grad()
        total_loss.backward()
        self.optimizer.step()
        
        return total_loss.item()

    def save_model(self):
        torch.save(self.model.state_dict(), self.model_path)
        print(f"Model saved to {self.model_path}")

    def load_model(self):
        if os.path.exists(self.model_path):
            try:
                state_dict = torch.load(self.model_path, map_location=self.device)
                
                # Ελέγχουμε αν υπάρχει το νέο BW head και ο σωστός αριθμός links
                if 'bw_head.2.bias' in state_dict and state_dict['link_head.2.bias'].shape[0] == self.num_links:
                    self.model.load_state_dict(state_dict)
                    print("Loaded existing ML model.")
                else:
                    print("Architecture changed (Added Bandwidth Head). Starting fresh.")
            except Exception as e:
                print(f"Could not load model: {e}")
        else:
            print("No existing model found. Starting fresh.")