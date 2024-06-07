import numpy as np
import torch as th
import matplotlib.pyplot as plt
from torch import nn
from torch import optim

TEN = th.Tensor
ARY = np.ndarray

'''data process'''


def generate_cum_normal_noise(num_points: int = 240, sigma: float = 0.5) -> ARY:  # simulation of price
    noise = np.random.normal(0, sigma, num_points)
    noise = np.cumsum(noise)
    return noise


def generate_exp_normal_noise(num_points: int = 240, sigma: float = 0.5) -> ARY:  # simulation of volume
    noise = np.random.normal(0, sigma, num_points)
    noise = np.exp(noise)
    return noise


def plot_generative_noise():
    num_points = 240
    sigma = 0.5

    noise = generate_cum_normal_noise(num_points, sigma)
    plt.plot(noise, label='cum_normal_noise: price')
    noise = generate_exp_normal_noise(num_points, sigma)
    plt.plot(noise, label='exp_normal_noise: volume')

    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title('Noise')
    plt.legend()
    plt.grid()
    plt.show()


def calculate_vwap_5min():
    num_points = 240  # 240 minutes per day
    sigma = 0.5

    '''synthetic data'''
    average_price = generate_exp_normal_noise(num_points, sigma)
    volume = generate_exp_normal_noise(num_points, sigma)
    amount = volume * average_price

    """complete the following code: (encouraged to use search engines or ChatGPT)
    Based on 240 data points, calculate a VWAP (Volume-Weighted Average Price) every 5 minutes, 
    resulting in a total of 240/5==48 VWAP
    
    Only allowed to use numpy, pandas, pytorch, or Python's built-in libraries.
    """
    WINDOW = 5
    vwap = np.zeros(num_points // WINDOW)

    for i in range(0, num_points, WINDOW):
        win_volume = volume[i:i+WINDOW].sum()
        win_amount = amount[i:i+WINDOW].sum()
        vwap[i // WINDOW] = win_amount / win_volume if win_volume != 0 else 0

    return vwap

'''deep learning'''


def generate_time_series(if_plot: bool = False, num_periods: int = 4):
    xs = th.linspace(0, 2 * th.pi * num_periods, 128 * num_periods)
    ys = th.sin(xs) + 0.1 * xs
    ys += th.randn_like(ys) * 0.1

    if if_plot:
        plt.plot(th.arange(ys.shape[0]).numpy(), ys.numpy())
        plt.xlabel('x')
        plt.ylabel('y')
        plt.title('time series: y=(sin(x) + 0.1*x)+noise')
        plt.grid()
        plt.show()
    return ys


class NeuralNetwork(nn.Module):
    def __init__(self, input_dim: int, middle_dim: int, output_dim: int, num_layers: int = 1):
        super().__init__()
        self.rnn = nn.LSTM(input_dim, middle_dim, num_layers)
        self.mlp = nn.Linear(middle_dim, output_dim)

    def forward(self, inp, hid=None):
        tmp, hid = self.rnn(inp, hid)
        out = self.mlp(tmp)
        return out, hid

def prepare_datasets(data, sequence_len, pred_step):
    X, y =[], []
    for i in range(len(data) - sequence_len - pred_step):
        X.append(data[i: i + sequence_len])
        y.append(data[i + sequence_len + pred_step])
    return th.stack(X), th.stack(y)

def train_and_evaluate_network():
    gpu_id = 1
    if th.cuda.is_available():
        print("GPU is available. ")
    device = th.device(f'cuda:{gpu_id}' if th.cuda.is_available() and gpu_id >= 0 else 'cpu')
    ys = generate_time_series(if_plot=False)

    num_steps = 120
    """complete the following code: (encouraged to use search engines or ChatGPT)
    Based on the function `generate_time_series()`, generate sufficient data for training a neural network, 
    and then forecast the outcomes of this time series for the next 0 to 120 steps.
    
    There are multiple solutions to this problem, so the code below can be modified in any way.   
    """

    '''train'''
    sequence_len = 100
    pred_step = 1
    X, y = prepare_datasets(ys, sequence_len, pred_step)

    train_size = int(0.8 * len(X))
    X_train, y_train = X[:train_size], y[:train_size]
    X_test, y_test = X[train_size:], y[train_size:]

    network = NeuralNetwork(input_dim=1, middle_dim=50, output_dim = 1, num_layers=1)
    loss_type = nn.MSELoss()
    optimizer = optim.Adam(network.parameters(), lr=0.001)

    train_times = 500
    print_gap = 50
    for i in range(train_times):
        objective = 0

        for X_batch, y_batch in zip(X_train, y_train):
            X_batch = X_batch.to(device)
            y_batch = y_batch.to(device)
            
            optimizer.zero_grad()
            outputs, _ = network(X_batch.view(-1, 1, 1))
            batch_loss = loss_type(outputs, y_batch)

            batch_loss.backward()
            optimizer.step()
            objective += batch_loss.item()

        if i % print_gap == 0:
            loss = objective #objective: loss?
            print(f"{i:9}  loss {loss:9.3f}")

    '''evaluate'''
    output_tensor, _ = network(X_test[0].view(-1, 1, 1))
    ys_predict = output_tensor[:, 0, 0]

    plt_xs = th.arange(ys.shape[0]).numpy()
    plt_ys = ys.numpy()
    plt.plot(plt_xs, plt_ys, label='raw data')

    plt_xs = th.arange(ys_predict.shape[0]).detach().numpy() + num_steps
    plt_ys = ys_predict.detach().numpy()
    plt.plot(plt_xs, plt_ys, label='predict')

    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('time series: y=(sin(x) + 0.1*x)+noise')
    plt.legend()
    plt.grid()
    plt.show()


if __name__ == '__main__':
    calculate_vwap_5min()  # task 1
    train_and_evaluate_network()  # task 2
